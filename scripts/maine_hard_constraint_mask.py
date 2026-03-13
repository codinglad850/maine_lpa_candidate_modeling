import time
from typing import Dict, List, Optional, Tuple

from src.sources.constraints import (
    CONSTRAINT_POLYGON_SOURCES,
    NOAA_NAV_SOURCES,
    AQ_SOURCES
)

from src.paths import HARD_MASK

import requests
import pandas as pd
import geopandas as gpd

# ============================================================
# CONFIG
# ============================================================

# Buffer distance: 1000 feet -> meters
FEET_TO_METERS = 0.3048
NOAA_BUFFER_M = 1000.0 * FEET_TO_METERS  # 304.8 m

# Clip web downloads to Casco Bay bbox (lon/lat): xmin, ymin, xmax, ymax
# Adjust if you want a different AOI.
BBOX_4326 = (-71.10, 42.95, -66.85, 47.50)

# CRS for buffering and geometry operations (meters)
METRIC_CRS = "EPSG:26919"

include_aquaculture = True

# Output
if include_aquaculture:
    OUT_MASK_GPKG = HARD_MASK
else:
    OUT_MASK_GPKG = "me_geojson/combined_hard_constraint_mask_ex_aq.gpkg"

# ============================================================
# REST HELPERS
# ============================================================

session = requests.Session()
session.headers.update({"User-Agent": "casco-hard-mask-builder/1.0"})

def _get_json(url: str, params: Dict, retries: int = 5, timeout: int = 120) -> Dict:
    last = None
    for i in range(retries):
        try:
            r = session.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            time.sleep(2 ** i)
    raise RuntimeError(f"Request failed: {url} last={last}")


def arcgis_layer_meta(layer_url: str) -> Dict:
    params = {"f": "pjson"}
    return _get_json(layer_url, params)


def arcgis_query_geojson(
    layer_url: str,
    where: str = "1=1",
    bbox_4326: Optional[Tuple[float, float, float, float]] = None,
    out_fields: str = "*",
) -> Dict:
    """
    Downloads a FeatureServer layer as GeoJSON with pagination, optionally clipped to bbox.
    """
    meta = arcgis_layer_meta(layer_url)
    max_rc = int(meta.get("maxRecordCount") or 2000)

    qurl = layer_url.rstrip("/") + "/query"
    offset = 0
    feats_all = []

    while True:
        params = {
            "where": where,
            "outFields": out_fields,
            "returnGeometry": "true",
            "outSR": 4326,
            "f": "geojson",
            "resultOffset": offset,
            "resultRecordCount": max_rc,
        }

        if bbox_4326:
            xmin, ymin, xmax, ymax = bbox_4326
            params.update({
                "geometry": f"{xmin},{ymin},{xmax},{ymax}",
                "geometryType": "esriGeometryEnvelope",
                "inSR": 4326,
                "spatialRel": "esriSpatialRelIntersects",
            })

        data = _get_json(qurl, params)
        feats = data.get("features", [])
        feats_all.extend(feats)

        if len(feats) < max_rc:
            break

        offset += max_rc
        if offset > 10_000_000:
            raise RuntimeError(f"Pagination runaway for {layer_url}")

    return {"type": "FeatureCollection", "features": feats_all}


def geojson_to_gdf(fc: Dict) -> gpd.GeoDataFrame:
    if not fc.get("features"):
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
    gdf = gpd.GeoDataFrame.from_features(fc["features"], crs="EPSG:4326")
    gdf = gdf[~gdf.geometry.isna()].copy()
    return gdf


# ============================================================
# MASK BUILDING
# ============================================================

def _read_local_mask(path: str) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(path)
    if gdf.crs is None:
        # GeoJSON is usually WGS84 if unspecified
        gdf = gdf.set_crs("EPSG:4326")
    return gdf


def _download_layer_as_gdf(name: str, url: str) -> gpd.GeoDataFrame:
    fc = arcgis_query_geojson(url, bbox_4326=BBOX_4326)
    gdf = geojson_to_gdf(fc)
    if not gdf.empty:
        gdf["__src_name"] = name
        gdf["__src_url"] = url
    return gdf


def _buffer_if_needed(gdf_metric: gpd.GeoDataFrame, buffer_m: float) -> gpd.GeoDataFrame:
    """
    Buffer any geometries (points/lines/polygons) by buffer_m.
    - For polygons, buffer expands them as well (desired for “keep away” zones).
    """
    if buffer_m <= 0:
        return gdf_metric

    out = gdf_metric.copy()
    out["geometry"] = out.geometry.buffer(buffer_m)
    # drop empties
    out = out[~out.geometry.is_empty & out.geometry.notna()].copy()
    return out


def main():

    parts: List[gpd.GeoDataFrame] = []

    if include_aquaculture:
        # Download existing aquaculture operations and LPAs, buffer 1000 ft
        for src in AQ_SOURCES:
            print(f"Downloading {src.key} ...")
            gdf = _download_layer_as_gdf(src.key, src.url)
            if gdf.empty:
                print(f"  -> no features returned (check bbox): {src.key}")
                continue

            gdf_m = gdf.to_crs(METRIC_CRS)
            gdf_m = _buffer_if_needed(gdf_m, NOAA_BUFFER_M)

            gdf_buff = gdf_m.to_crs("EPSG:4326")
            gdf_buff["__src_name"] = f"{src.key}_buffer1000ft"
            gdf_buff["__src_url"] = src.url

            if src is LPA_LAYER:
                gdf_buff = gdf_buff[gdf_buff["Status"] == "A"]
                print(f"obtained {len(gdf_buff.index)} active LPAs")
            elif src is FULL_AQUACULTURE_LAYER:
                gdf_buff = gdf_buff[
                    (gdf_buff["STATUS"] == "A") | (gdf_buff["STATUS"] == "P")
                    ]
                print(f"obtained {len(gdf_buff.index)} full active aquaculture operations")
            else:
                raise RuntimeError(f"Unhandled aquaculture source: {src.key}")

            parts.append(gdf_buff)


    # 1) Load local masks

    for src in HARD_CONSTRAINT_LOCAL_SOURCES.values():

        p = src.path_obj()

        if not p.exists():
            raise FileNotFoundError(f"{src.key} not found: {p}")

        gdf = _read_local_mask(str(p))

        gdf["__src_name"] = src.key
        gdf["__src_url"] = f"file://{p.resolve()}"

        parts.append(gdf)

    # 2) Download the polygon habitat layers
    for src in CONSTRAINT_POLYGON_SOURCES:
        print(f"Downloading {src.key} ...")
        gdf = _download_layer_as_gdf(src.key, src.url)
        if gdf.empty:
            print(f"  -> no features returned (check bbox/permissions): {src.key}")
            continue

        gdf["__src_name"] = src.key
        gdf["__src_url"] = src.url
        parts.append(gdf)

    # 3) Download NOAA navigation layers and buffer 1000 ft
    for src in NOAA_NAV_SOURCES:
        print(f"Downloading {src.key} ...")
        gdf = _download_layer_as_gdf(src.key, src.url)
        if gdf.empty:
            print(f"  -> no features returned (check bbox): {src.key}")
            continue

        # project to metric and buffer
        gdf_m = gdf.to_crs(METRIC_CRS)
        gdf_m = _buffer_if_needed(gdf_m, NOAA_BUFFER_M)

        # return to WGS84 for later concat (we'll union in metric though)
        gdf_buff = gdf_m.to_crs("EPSG:4326")
        gdf_buff["__src_name"] = f"{src.key}_buffer1000ft"
        gdf_buff["__src_url"] = src.url
        parts.append(gdf_buff)

    # 4) Combine + dissolve/union everything into one geometry in metric CRS
    if not parts:
        raise RuntimeError("No mask parts found.")

    combined = pd.concat(parts, ignore_index=True)
    combined = gpd.GeoDataFrame(combined, geometry="geometry", crs=parts[0].crs)

    # project to metric for union
    combined_m = combined.to_crs(METRIC_CRS)

    print("generate combined geometry geodataframe...")

    all_geoms = [g for g in combined_m.geometry if g is not None and not g.is_empty]
    out = gpd.GeoDataFrame(
        geometry=all_geoms,
        crs=METRIC_CRS
    ).to_crs("EPSG:4326")

    # Analysis copy (recommended)
    out.to_file(OUT_MASK_GPKG, layer="mask", driver="GPKG")

    print(f"✅ Wrote {OUT_MASK_GPKG}")


if __name__ == "__main__":
    main()