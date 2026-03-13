import re
import hashlib
from pathlib import Path

import pandas as pd
import geopandas as gpd
import requests

from src.sources.common import SourceBase, RemoteSource, LocalSource

from src.paths import ACCESS_GIS_CACHE_DIR, NORMALIZED_ACCESS_POINTS

from src.sources.access import ACCESS_ALL_SOURCES


WGS84 = "EPSG:4326"
METRIC_CRS = "EPSG:26919"  # Maine UTM 19N

# ---------------------------
# CONFIG
# ---------------------------


# Scoring
D0_METERS = 1500.0

HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
}

ARCGIS_BATCH = 2000

def is_arcgis_layer_url(s: str) -> bool:
    return bool(re.search(r"/(FeatureServer|MapServer)/\d+/?$", s))

def cache_path_for_url(url: str, suffix: str) -> Path:
    h = hashlib.sha256(url.encode("utf-8")).hexdigest()[:24]
    return ACCESS_GIS_CACHE_DIR / f"{h}{suffix}"

def clean_geometries(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    gdf = gdf[gdf.geometry.notnull()]
    gdf = gdf[~gdf.geometry.is_empty]
    return gdf.reset_index(drop=True)

def ensure_crs(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if gdf.crs is None:
        return gdf.set_crs(WGS84)
    return gdf

def force_points(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    gdf = clean_geometries(gdf).copy()

    poly_mask = gdf.geometry.geom_type.isin(["Polygon", "MultiPolygon"])
    line_mask = gdf.geometry.geom_type.isin(["LineString", "MultiLineString"])

    if poly_mask.any() or line_mask.any():
        gdf.loc[poly_mask | line_mask, "geometry"] = gdf.loc[poly_mask | line_mask, "geometry"].representative_point()

    gdf = gdf[gdf.geometry.geom_type.isin(["Point", "MultiPoint"])].copy()
    return clean_geometries(gdf)

def fetch_arcgis_geojson_all(layer_url: str, where="1=1", out_fields="*", page_size=ARCGIS_BATCH, timeout=60) -> gpd.GeoDataFrame:
    query_url = f"{layer_url}/query"
    sess = requests.Session()

    r = sess.get(
        query_url,
        params={"where": where, "returnCountOnly": "true", "f": "json"},
        headers=HTTP_HEADERS,
        timeout=timeout,
    )
    r.raise_for_status()
    js = r.json()
    total = int(js.get("count", 0))

    if total == 0:
        return gpd.GeoDataFrame(geometry=[], crs=WGS84)

    chunks = []
    offset = 0

    while offset < total:
        params = {
            "where": where,
            "outFields": out_fields,
            "returnGeometry": "true",
            "outSR": 4326,
            "f": "geojson",
            "resultOffset": offset,
            "resultRecordCount": page_size,
        }

        rr = sess.get(query_url, params=params, headers=HTTP_HEADERS, timeout=timeout)
        rr.raise_for_status()
        gj = rr.json()

        if "error" in gj:
            raise RuntimeError(f"ArcGIS error from {layer_url}: {gj['error']}")

        feats = gj.get("features", [])
        if not feats:
            break

        gdf = gpd.GeoDataFrame.from_features(feats, crs=WGS84)
        chunks.append(gdf)

        got = len(gdf)
        offset += got

        if got < page_size:
            break

    out = gpd.GeoDataFrame(pd.concat(chunks, ignore_index=True), crs=WGS84)
    return clean_geometries(out)

def load_access_source(src: SourceBase) -> gpd.GeoDataFrame:
    if isinstance(src, RemoteSource):
        url = src.url

        if is_arcgis_layer_url(url):
            cache_fp = cache_path_for_url(url, ".geojson")
            if cache_fp.exists() and cache_fp.stat().st_size > 0:
                gdf = gpd.read_file(cache_fp)
            else:
                gdf = fetch_arcgis_geojson_all(url)
                gdf = ensure_crs(gdf).to_crs(WGS84)
                cache_fp.parent.mkdir(parents=True, exist_ok=True)
                gdf.to_file(cache_fp, driver="GeoJSON")

            gdf = ensure_crs(gdf).to_crs(WGS84)
            gdf["__src_name"] = src.key
            gdf["__src_url"] = src.url
            return gdf

        cache_fp = cache_path_for_url(url, ".geojson")
        if not cache_fp.exists() or cache_fp.stat().st_size == 0:
            cache_fp.parent.mkdir(parents=True, exist_ok=True)
            r = requests.get(url, headers=HTTP_HEADERS, timeout=60)
            r.raise_for_status()
            cache_fp.write_bytes(r.content)

        gdf = gpd.read_file(cache_fp)
        gdf = ensure_crs(gdf).to_crs(WGS84)
        gdf["__src_name"] = src.key
        gdf["__src_url"] = src.url
        return gdf

    if isinstance(src, LocalSource):
        p = src.path_obj()
        gdf = gpd.read_file(p)
        gdf = ensure_crs(gdf).to_crs(WGS84)
        gdf["__src_name"] = src.key
        gdf["__src_path"] = str(p.resolve())
        return gdf
    print(src)
    raise TypeError(f"Unsupported source type for {src.key}: {type(src)}")

def main():
    access_parts = []
    for src in ACCESS_ALL_SOURCES.values():
        gdf = load_access_source(src)
        gdf = force_points(gdf)
        if len(gdf) == 0:
            print(f"Warning: no usable point geometries found in access source: {src}")
            continue
        access_parts.append(gdf)

    if not access_parts:
        raise RuntimeError("No access points loaded. Check ACCESS_SOURCES.")

    access = gpd.GeoDataFrame(pd.concat(access_parts, ignore_index=True), crs=WGS84)
    access['access_point_id'] = range(1, access.shape[0] + 1)
    access.to_file(NORMALIZED_ACCESS_POINTS, driver="GeoJSON")

if __name__ == "__main__":
    main()
