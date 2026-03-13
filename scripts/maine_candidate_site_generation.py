import time
from typing import Dict, Optional, Tuple

import requests
import geopandas as gpd
from shapely.geometry import Point

from src.paths import HARD_MASK, CANDIDATES_SCREENED
from src.sources.candidate_generation import NSSP_APPROVED_AREAS

# =========================
# CONFIG
# =========================

NSSP_WHERE = "NSSP = 'A'"  # Approved only (no union needed)

print(f'executing with hard constraints: {HARD_MASK}')

GRID_SPACING_M = 250.0

METRIC_CRS = "EPSG:26919"
WGS84 = "EPSG:4326"

# optional download bbox
BBOX_4326: Optional[Tuple[float, float, float, float]] = (-71.10, 42.95, -66.85, 47.50)

ARCGIS_TOKEN = None

# =========================
# REST helpers
# =========================

session = requests.Session()
session.headers.update({"User-Agent": "unionfree-aoi-grid/1.0"})

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
    if ARCGIS_TOKEN:
        params["token"] = ARCGIS_TOKEN
    return _get_json(layer_url, params)

def arcgis_query_geojson(layer_url: str, where: str) -> Dict:
    meta = arcgis_layer_meta(layer_url)
    max_rc = int(meta.get("maxRecordCount") or 2000)

    qurl = layer_url.rstrip("/") + "/query"
    offset = 0
    feats_all = []

    while True:
        params = {
            "where": where,
            "outFields": "*",
            "returnGeometry": "true",
            "outSR": 4326,
            "f": "geojson",
            "resultOffset": offset,
            "resultRecordCount": max_rc,
        }
        if ARCGIS_TOKEN:
            params["token"] = ARCGIS_TOKEN

        if BBOX_4326:
            xmin, ymin, xmax, ymax = BBOX_4326
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

    return {"type": "FeatureCollection", "features": feats_all}

def geojson_to_gdf(fc: Dict) -> gpd.GeoDataFrame:
    if not fc.get("features"):
        return gpd.GeoDataFrame(geometry=[], crs=WGS84)
    gdf = gpd.GeoDataFrame.from_features(fc["features"], crs=WGS84)
    return gdf[~gdf.geometry.isna()].copy()

# =========================
# Grid helper
# =========================

def frange(start: float, stop: float, step: float):
    x = start
    while x <= stop:
        yield x
        x += step

def grid_over_bounds(bounds, spacing_m: float):
    minx, miny, maxx, maxy = bounds
    xs = list(frange(minx, maxx, spacing_m))
    ys = list(frange(miny, maxy, spacing_m))
    pts = [Point(x, y) for y in ys for x in xs]
    return pts

# =========================
# Main
# =========================

def main():
    # 1) Approved polygons (no dissolve/union)
    print(f"Loading source: {NSSP_APPROVED_AREAS.key}")
    print(f"URL: {NSSP_APPROVED_AREAS.url}")
    fc = arcgis_query_geojson(NSSP_APPROVED_AREAS.url, where=NSSP_WHERE)
    approved = geojson_to_gdf(fc)
    if approved.empty:
        raise RuntimeError("No approved polygons returned. Check NSSP_WHERE.")
    approved = approved[approved.geometry.type.isin(["Polygon", "MultiPolygon"])].copy()

    # Fix invalids (helps overlay/sjoin correctness)
    approved_m = approved.to_crs(METRIC_CRS)
    approved_m["geometry"] = approved_m.geometry.make_valid()

    # 2) Load hard mask (no union)
    mask = gpd.read_file(HARD_MASK)
    if mask.crs is None:
        mask = mask.set_crs(WGS84)
    mask = mask[mask.geometry.type.isin(["Polygon", "MultiPolygon"])].copy()
    mask_m = mask.to_crs(METRIC_CRS)
    mask_m["geometry"] = mask_m.geometry.make_valid()

    # 3) Clip mask to approved AOI using overlay intersection (still union-free)
    #    This keeps mask smaller for later point filtering.
    mask_clip_m = gpd.overlay(mask_m, approved_m[["geometry"]], how="intersection", keep_geom_type=True)

    # 4) Generate a grid over APPROVED BOUNDS (rectangle), then filter by within-approved
    bounds = approved_m.total_bounds  # in meters CRS
    grid_pts = grid_over_bounds(bounds, GRID_SPACING_M)
    candidates_m = gpd.GeoDataFrame(
        {"candidate_id": range(1, len(grid_pts) + 1)},
        geometry=grid_pts,
        crs=METRIC_CRS
    )

    # 5) Keep only points within Approved polygons (spatial join)
    candidates_in_approved = gpd.sjoin(
        candidates_m,
        approved_m[["geometry"]],
        how="inner",
        predicate="within"
    ).drop(columns=["index_right"])

    # 6) Remove points within clipped hard mask (anti-join)
    # left join to mask; keep rows with no match
    tmp = gpd.sjoin(
        candidates_in_approved,
        mask_clip_m[["geometry"]],
        how="left",
        predicate="within"
    )
    candidates_ok = tmp[tmp["index_right"].isna()].drop(columns=["index_right"])

    # reset IDs after filtering (optional)
    candidates_ok = candidates_ok.reset_index(drop=True)
    candidates_ok["candidate_id"] = range(1, len(candidates_ok) + 1)

    out = candidates_ok.to_crs(WGS84)
    out.to_file(CANDIDATES_SCREENED, driver="GeoJSON")
    print(f"✅ Wrote {CANDIDATES_SCREENED} with {len(out):,} points")

if __name__ == "__main__":
    main()