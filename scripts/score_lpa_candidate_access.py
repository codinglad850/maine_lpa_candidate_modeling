#!/usr/bin/env python3

import re
import hashlib
from pathlib import Path

import numpy as np
import pandas as pd
import geopandas as gpd
import requests

from src.sources.common import SourceBase, RemoteSource, LocalSource

from src.paths import (
    CANDIDATES_INTERTIDAL,
    NORMALIZED_ACCESS_POINTS,
    CANDIDATE_ACCESS_SCORED,
    CANDIDATE_ACCESS_SCORED_CSV,
    LPA_ACCESS_SCORED,
    LPA_ACCESS_SCORED_CSV,
)
from src.sources.constraints import LPA_LAYER
from src.sources.access import TOWNS_LAYER


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


# ---------------------------
# Utilities
# ---------------------------

def ensure_crs(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if gdf.crs is None:
        return gdf.set_crs(WGS84)
    return gdf


def clean_geometries(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    gdf = gdf[gdf.geometry.notnull()]
    gdf = gdf[~gdf.geometry.is_empty]
    return gdf.reset_index(drop=True)


def force_points(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    gdf = clean_geometries(gdf).copy()

    point_mask = gdf.geometry.geom_type.isin(["Point", "MultiPoint"])
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


def access_score_exp(distance_m: pd.Series, d0: float) -> pd.Series:
    return np.exp(-distance_m / d0)


def pick_id_column(gdf: gpd.GeoDataFrame, preferred: list[str], fallback_name: str) -> tuple[gpd.GeoDataFrame, str]:
    for col in preferred:
        if col in gdf.columns:
            return gdf, col
    gdf = gdf.copy()
    gdf[fallback_name] = np.arange(1, len(gdf) + 1)
    return gdf, fallback_name


def assign_town(points_metric: gpd.GeoDataFrame, towns_metric: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    joined = gpd.sjoin(
        points_metric,
        towns_metric[["town", "geometry"]].copy(),
        how="left",
        predicate="within",
    ).drop(columns=["index_right"], errors="ignore")
    return joined


def load_towns() -> gpd.GeoDataFrame:
    towns = fetch_arcgis_geojson_all(TOWNS_LAYER.url)
    towns = ensure_crs(towns).to_crs(METRIC_CRS)

    name_candidates = ["TOWN", "TOWN_NAME", "MCD", "MCD_NAME", "COMMUNITY", "NAME"]
    town_name_field = next((c for c in name_candidates if c in towns.columns), None)
    if town_name_field is None:
        raise RuntimeError(f"Could not find a town name field. Available columns: {list(towns.columns)}")

    towns = towns[[town_name_field, "geometry"]].rename(columns={town_name_field: "town"}).copy()
    towns["town"] = towns["town"].astype(str).str.strip().str.title()
    return towns


def score_points(points_gdf: gpd.GeoDataFrame, access_metric: gpd.GeoDataFrame, point_type: str, id_col: str, towns_metric: gpd.GeoDataFrame | None = None) -> gpd.GeoDataFrame:
    points_metric = ensure_crs(points_gdf).to_crs(METRIC_CRS)
    points_metric = force_points(points_metric)

    nearest = gpd.sjoin_nearest(
        points_metric,
        access_metric[["geometry"]].copy(),
        how="left",
        distance_col="d_access_m",
    )

    points_metric["d_access_m"] = nearest["d_access_m"].astype(float)
    points_metric["access_score"] = access_score_exp(points_metric["d_access_m"], D0_METERS)
    points_metric["point_type"] = point_type

    if towns_metric is not None:
        points_metric = assign_town(points_metric, towns_metric)

    keep_front = [c for c in [id_col, "point_type", "town", "d_access_m", "access_score"] if c in points_metric.columns]
    other_cols = [c for c in points_metric.columns if c not in keep_front + ["geometry"]]
    points_metric = points_metric[keep_front + other_cols + ["geometry"]]

    return points_metric


# ---------------------------
# Main
# ---------------------------
def main():
    # 1) candidates
    cand = gpd.read_file(CANDIDATES_INTERTIDAL)
    cand = ensure_crs(cand).to_crs(WGS84)
    cand = clean_geometries(cand)
    cand = force_points(cand)
    cand, cand_id_col = pick_id_column(
        cand,
        preferred=["spot_id", "id", "candidate_id"],
        fallback_name="candidate_id"
    )

    # 2) LPAs
    lpa = fetch_arcgis_geojson_all(LPA_LAYER.url)
    lpa = ensure_crs(lpa).to_crs(WGS84)
    lpa = clean_geometries(lpa)
    lpa = force_points(lpa)
    lpa, lpa_id_col = pick_id_column(
        lpa,
        preferred=["SITE_ID", "site_id", "LICENSE_ID", "id"],
        fallback_name="lpa_id"
    )

    access = gpd.read_file(NORMALIZED_ACCESS_POINTS)
    access = access.to_crs(METRIC_CRS)
    # 4) towns
    towns_metric = load_towns()

    # 5) score candidates
    cand_scored = score_points(
        cand,
        access_metric=access,
        point_type="candidate",
        id_col=cand_id_col,
        towns_metric=towns_metric,
    )

    # 6) score LPAs
    lpa_scored = score_points(
        lpa,
        access_metric=access,
        point_type="lpa",
        id_col=lpa_id_col,
        towns_metric=towns_metric,
    )

    # exports
    cand_scored_wgs84 = cand_scored.to_crs(WGS84)
    cand_scored_wgs84.to_file(CANDIDATE_ACCESS_SCORED, driver="GeoJSON")

    cand_csv = cand_scored_wgs84.copy()
    cand_csv["lon"] = cand_csv.geometry.x
    cand_csv["lat"] = cand_csv.geometry.y
    cand_csv.drop(columns="geometry").to_csv(CANDIDATE_ACCESS_SCORED_CSV, index=False)

    lpa_scored_wgs84 = lpa_scored.to_crs(WGS84)
    lpa_scored_wgs84.to_file(LPA_ACCESS_SCORED, driver="GeoJSON")

    lpa_csv = lpa_scored_wgs84.copy()
    lpa_csv["lon"] = lpa_csv.geometry.x
    lpa_csv["lat"] = lpa_csv.geometry.y
    lpa_csv.drop(columns="geometry").to_csv(LPA_ACCESS_SCORED_CSV, index=False)

    print("Wrote:")
    print(" -", CANDIDATE_ACCESS_SCORED)
    print(" -", CANDIDATE_ACCESS_SCORED_CSV)
    print(" -", LPA_ACCESS_SCORED)
    print(" -", LPA_ACCESS_SCORED_CSV)

if __name__ == "__main__":
    main()