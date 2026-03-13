from __future__ import annotations

import time
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import requests
from scipy.spatial import cKDTree

from src.paths import (
    CANDIDATE_ACCESS_SCORED,
    LPA_ACCESS_SCORED,
)
from src.sources.common import RemoteSource
from src.sources.model_inputs import PARCEL_GDB, SHORELINE
from src.sources.constraints import LPA_LAYER, FULL_AQUACULTURE_LAYER


TARGET_CRS = "EPSG:26919"
WGS84 = "EPSG:4326"

ARCGIS_BATCH = 1000
BACKGROUND_MULTIPLIER = 2
BACKGROUND_MIN = 1000
BACKGROUND_AWAY_FROM_LPA_M = 250.0
TOWN_SMOOTHING_K = 25.0

HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
}

FEATURES = [
    "homes_500m",
    "homes_1000m",
    "log_mean_land_value",
    "log_distance_to_shore",
    "log_dist_lease_cluster",
    "town_logit_prior",
    "access_score",
]


# -----------------------------------------------------
# Geometry cleanup
# -----------------------------------------------------

def clean_geometries(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    gdf = gdf[gdf.geometry.notnull()]
    gdf = gdf[~gdf.geometry.is_empty]
    return gdf.reset_index(drop=True)


# -----------------------------------------------------
# Safe coordinate extraction
# -----------------------------------------------------

def gdf_coords(gdf: gpd.GeoDataFrame) -> tuple[np.ndarray, np.ndarray]:
    x = gdf.geometry.x.values
    y = gdf.geometry.y.values
    mask = np.isfinite(x) & np.isfinite(y)
    coords = np.column_stack((x[mask], y[mask]))
    return coords, mask


# -----------------------------------------------------
# Load shoreline and clip to Maine
# -----------------------------------------------------

def load_shoreline() -> gpd.GeoDataFrame:
    print("Loading shoreline")
    shore = gpd.read_file(SHORELINE.path_obj())
    if shore.crs is None:
        raise ValueError("Shoreline has no CRS defined.")

    shore = shore.to_crs(WGS84)
    shore = shore.cx[-71.2:-66.5, 42.9:47.5]
    shore = shore.to_crs(TARGET_CRS)
    shore = clean_geometries(shore)

    print("Shoreline features:", len(shore))
    return shore


# -----------------------------------------------------
# Load parcels from GDB and join valuation attributes
# -----------------------------------------------------

def load_parcels() -> gpd.GeoDataFrame:
    print("Loading parcel geometry")
    geom = gpd.read_file(PARCEL_GDB.path_obj(), layer="Parcels")

    print("Loading parcel attributes")
    adb = gpd.read_file(PARCEL_GDB.path_obj(), layer="PARCELS_ADB")

    geom["STATE_ID"] = geom["STATE_ID"].astype(str).str.strip()
    adb["STATE_ID"] = adb["STATE_ID"].astype(str).str.strip()
    adb = adb.drop_duplicates("STATE_ID")

    keep_cols = [c for c in ["STATE_ID", "TOWN", "geometry"] if c in geom.columns]
    geom = geom[keep_cols].copy()

    parcels = geom.merge(
        adb[[c for c in ["STATE_ID", "LAND_VAL"] if c in adb.columns]],
        on="STATE_ID",
        how="left",
    )

    if "LAND_VAL" not in parcels.columns:
        raise ValueError("LAND_VAL was not found after joining PARCELS_ADB.")

    parcels = parcels.to_crs(TARGET_CRS)
    parcels["LAND_VAL"] = pd.to_numeric(parcels["LAND_VAL"], errors="coerce")
    parcels = parcels[parcels["LAND_VAL"].notnull()].copy()

    parcels["geometry"] = parcels.geometry.representative_point()
    parcels = clean_geometries(parcels)

    if "TOWN" not in parcels.columns:
        parcels["TOWN"] = "Unknown"
    else:
        parcels["TOWN"] = parcels["TOWN"].astype(str).str.strip().str.title()

    print("Parcels loaded:", len(parcels))
    parcels = parcels[parcels["TOWN"] != '']
    print("Parcels loaded, dropping blanks:", len(parcels))
    return parcels


# -----------------------------------------------------
# Generic ArcGIS paginated downloader
# -----------------------------------------------------

def arcgis_query_url(src: RemoteSource) -> str:
    url = src.url.rstrip("/")
    return url if url.endswith("/query") else f"{url}/query"


def download_arcgis_geojson_features(src: RemoteSource) -> gpd.GeoDataFrame:
    """
    Download all features from an ArcGIS FeatureServer / MapServer layer
    defined as a RemoteSource and return a GeoDataFrame in TARGET_CRS.
    """
    layer_url = arcgis_query_url(src)
    label = src.key

    print(f"Downloading {label}")

    features = []
    offset = 0

    while True:
        params = {
            "where": "1=1",
            "outFields": "*",
            "returnGeometry": "true",
            "f": "geojson",
            "resultOffset": offset,
            "resultRecordCount": ARCGIS_BATCH,
        }

        try:
            r = requests.get(
                layer_url,
                params=params,
                headers=HTTP_HEADERS,
                timeout=180,
            )
            r.raise_for_status()
        except requests.RequestException as e:
            raise RuntimeError(
                f"Request failed for {label} at offset {offset}: {e}"
            ) from e

        content_type = r.headers.get("Content-Type", "")
        if "json" not in content_type.lower():
            raise RuntimeError(
                f"Non-JSON response for {label} at offset {offset}\n"
                f"Status: {r.status_code}\n"
                f"Content-Type: {content_type}\n"
                f"Preview:\n{r.text[:500]}"
            )

        try:
            data = r.json()
        except requests.exceptions.JSONDecodeError as e:
            raise RuntimeError(
                f"Invalid JSON response for {label} at offset {offset}\n"
                f"Status: {r.status_code}\n"
                f"Preview:\n{r.text[:500]}"
            ) from e

        if "error" in data:
            raise RuntimeError(
                f"ArcGIS error for {label} at offset {offset}: {data['error']}"
            )

        batch = data.get("features", [])
        if not batch:
            break

        features.extend(batch)
        offset += ARCGIS_BATCH

        print(f"Downloaded {label}: {len(features)}")

        if len(batch) < ARCGIS_BATCH:
            break

        time.sleep(0.2)

    if not features:
        raise RuntimeError(f"No features downloaded for {label}")

    gdf = gpd.GeoDataFrame.from_features(features)
    gdf = gdf.set_crs(WGS84)
    gdf = gdf.to_crs(TARGET_CRS)
    gdf = clean_geometries(gdf)

    gdf["__src_key"] = src.key
    gdf["__src_name"] = src.name
    gdf["__src_url"] = src.url

    print(f"Total {label}: {len(gdf)}")
    return gdf


def download_lpa_sites() -> gpd.GeoDataFrame:
    lpa = download_arcgis_geojson_features(LPA_LAYER)

    if "Species" in lpa.columns:
        lpa = lpa[lpa["Species"].astype(str).str.contains("oyster", case=False, na=False)]

    if "Status" in lpa.columns:
        lpa = lpa[lpa["Status"] == "A"]

    lpa["decision"] = 1
    return clean_geometries(lpa)


def download_leases() -> gpd.GeoDataFrame:
    leases = download_arcgis_geojson_features(FULL_AQUACULTURE_LAYER)
    leases["geometry"] = leases.geometry.representative_point()
    leases = clean_geometries(leases)
    return leases


# -----------------------------------------------------
# Access-score loaders
# -----------------------------------------------------

def load_candidate_access_scores() -> pd.DataFrame:
    gdf = gpd.read_file(CANDIDATE_ACCESS_SCORED).to_crs(TARGET_CRS)

    required = {"candidate_id", "access_score"}
    missing = required - set(gdf.columns)
    if missing:
        raise ValueError(f"Missing candidate access columns: {sorted(missing)}")

    keep = [c for c in ["candidate_id", "d_access_m", "access_score"] if c in gdf.columns]
    return gdf[keep].drop_duplicates(subset=["candidate_id"])


def load_lpa_access_scores() -> pd.DataFrame:
    gdf = gpd.read_file(LPA_ACCESS_SCORED).to_crs(TARGET_CRS)

    required = {"OBJECTID", "access_score"}
    missing = required - set(gdf.columns)
    if missing:
        raise ValueError(f"Missing LPA access columns: {sorted(missing)}")

    keep = [c for c in ["OBJECTID", "d_access_m", "access_score"] if c in gdf.columns]
    return gdf[keep].drop_duplicates(subset=["OBJECTID"])


# -----------------------------------------------------
# Background sampling
# -----------------------------------------------------

def filter_background_away_from_lpa(
    candidate: gpd.GeoDataFrame,
    lpa: gpd.GeoDataFrame,
    min_dist: float = BACKGROUND_AWAY_FROM_LPA_M,
) -> gpd.GeoDataFrame:
    cand_coords, cand_mask = gdf_coords(candidate)
    candidate_use = candidate.iloc[cand_mask].copy()

    lpa_coords, lpa_mask = gdf_coords(lpa)
    lpa_use = lpa.iloc[lpa_mask].copy()

    if len(cand_coords) == 0 or len(lpa_coords) == 0:
        return candidate_use

    tree = cKDTree(lpa_coords)
    dist, _ = tree.query(cand_coords, k=1)

    return candidate_use.loc[dist >= min_dist].copy()


def generate_background(
    candidate_pool: gpd.GeoDataFrame,
    n_positive: int,
    background_multiplier: int = BACKGROUND_MULTIPLIER,
    background_min: int = BACKGROUND_MIN,
) -> gpd.GeoDataFrame:
    target_n = max(background_min, background_multiplier * n_positive)
    n = min(len(candidate_pool), target_n)

    if n == 0:
        raise ValueError("No background candidates available after filtering away from LPAs.")

    bg = candidate_pool.sample(n, random_state=1).copy()
    bg["decision"] = 0
    return bg


# -----------------------------------------------------
# KDTree helpers
# -----------------------------------------------------

def count_neighbors(tree: cKDTree, coords: np.ndarray, radius: float) -> np.ndarray:
    neighbors = tree.query_ball_point(coords, r=radius)
    return np.array([len(i) for i in neighbors], dtype=int)


def mean_value(tree: cKDTree, coords: np.ndarray, values: np.ndarray, radius: float) -> np.ndarray:
    neighbors = tree.query_ball_point(coords, r=radius)
    out = []
    for i in neighbors:
        if len(i) == 0:
            out.append(0.0)
        else:
            out.append(values[i].mean())
    return np.array(out, dtype=float)


# -----------------------------------------------------
# Distances
# -----------------------------------------------------

def distance_to_shore(points: gpd.GeoDataFrame, shoreline: gpd.GeoDataFrame) -> np.ndarray:
    shore_union = shoreline.geometry.union_all()
    return np.array([geom.distance(shore_union) for geom in points.geometry], dtype=float)


def distance_to_leases(points: gpd.GeoDataFrame, leases: gpd.GeoDataFrame) -> np.ndarray:
    coords, mask = gdf_coords(points)
    points_use = points.iloc[mask].copy()

    lease_coords, lease_mask = gdf_coords(leases)
    leases_use = leases.iloc[lease_mask].copy()

    if len(lease_coords) == 0:
        return np.full(len(points_use), np.nan)

    tree = cKDTree(lease_coords)
    dist, _ = tree.query(coords)
    return dist


# -----------------------------------------------------
# Town assignment
# -----------------------------------------------------

def assign_town_by_nearest_parcel(
    points: gpd.GeoDataFrame,
    parcels: gpd.GeoDataFrame,
    parcel_tree: cKDTree | None = None,
    parcel_coords: np.ndarray | None = None,
) -> gpd.GeoDataFrame:
    pts = points.copy()

    if parcel_coords is None or parcel_tree is None:
        parcel_coords, parcel_mask = gdf_coords(parcels)
        parcels_use = parcels.iloc[parcel_mask].copy()
        parcel_tree = cKDTree(parcel_coords)
    else:
        parcels_use = parcels

    point_coords, point_mask = gdf_coords(pts)
    pts = pts.iloc[point_mask].copy()

    _, idx = parcel_tree.query(point_coords, k=1)
    pts["town"] = parcels_use.iloc[idx]["TOWN"].values
    return pts


# -----------------------------------------------------
# Empirical town prior
# -----------------------------------------------------

def build_town_prior(
    candidate_full: gpd.GeoDataFrame,
    lpa: gpd.GeoDataFrame,
    k: float = TOWN_SMOOTHING_K,
) -> pd.DataFrame:
    cand_counts = candidate_full["town"].value_counts(dropna=True).rename("candidate_n")
    lpa_counts = lpa["town"].value_counts(dropna=True).rename("lpa_n")

    prior = pd.concat([cand_counts, lpa_counts], axis=1).fillna(0).reset_index()
    prior = prior.rename(columns={"index": "town"})

    prior["candidate_n"] = prior["candidate_n"].astype(int)
    prior["lpa_n"] = prior["lpa_n"].astype(int)

    prior["candidate_n"] += prior["lpa_n"]

    total_lpa = prior["lpa_n"].sum()
    total_cand = prior["candidate_n"].sum()
    state_rate = total_lpa / max(total_cand, 1)

    prior["town_rate"] = (
        prior["lpa_n"] + k * state_rate
    ) / (
        prior["candidate_n"] + k
    )

    eps = 1e-4
    prior["town_rate_clamped"] = prior["town_rate"].clip(eps, 1 - eps)
    prior["town_logit_prior"] = np.log(
        prior["town_rate_clamped"] / (1 - prior["town_rate_clamped"])
    )

    prior = prior.sort_values("town_logit_prior", ascending=False).reset_index(drop=True)
    return prior[["town", "candidate_n", "lpa_n", "town_rate", "town_logit_prior"]]


def attach_town_prior(points: gpd.GeoDataFrame, prior_df: pd.DataFrame) -> gpd.GeoDataFrame:
    out = points.merge(prior_df[["town", "town_logit_prior"]], on="town", how="left")
    fill_val = out["town_logit_prior"].median()
    if pd.isna(fill_val):
        fill_val = 0.0
    out["town_logit_prior"] = out["town_logit_prior"].fillna(fill_val)
    return out


# -----------------------------------------------------
# Feature extraction
# -----------------------------------------------------

def compute_features(
    points: gpd.GeoDataFrame,
    parcels: gpd.GeoDataFrame,
    shoreline: gpd.GeoDataFrame,
    leases: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    parcel_coords, parcel_mask = gdf_coords(parcels)
    parcels_use = parcels.iloc[parcel_mask].copy()
    parcel_tree = cKDTree(parcel_coords)
    parcel_values = parcels_use["LAND_VAL"].values

    coords, mask = gdf_coords(points)
    points = points.iloc[mask].copy()

    print("homes_500m")
    points["homes_500m"] = count_neighbors(parcel_tree, coords, 500)

    print("homes_1000m")
    points["homes_1000m"] = count_neighbors(parcel_tree, coords, 1000)

    print("mean_land_value")
    points["mean_land_value"] = mean_value(parcel_tree, coords, parcel_values, 1000)

    print("distance_to_shore")
    points["distance_to_shore"] = distance_to_shore(points, shoreline)

    print("distance_to_leases")
    points["dist_lease_cluster"] = distance_to_leases(points, leases)

    print("assigning town")
    points = assign_town_by_nearest_parcel(
        points,
        parcels_use,
        parcel_tree=parcel_tree,
        parcel_coords=parcel_coords,
    )

    return points


def add_log_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["log_mean_land_value"] = np.log1p(out["mean_land_value"])
    out["log_distance_to_shore"] = np.log1p(out["distance_to_shore"])
    out["log_dist_lease_cluster"] = np.log1p(out["dist_lease_cluster"])
    return out


# -----------------------------------------------------
# Feature-table builders
# -----------------------------------------------------

def build_training_features(
    candidate: gpd.GeoDataFrame,
    parcels: gpd.GeoDataFrame,
    shoreline: gpd.GeoDataFrame,
    lpa: gpd.GeoDataFrame,
    leases: gpd.GeoDataFrame,
) -> tuple[gpd.GeoDataFrame, pd.DataFrame]:
    print("Assigning towns to full candidate set")
    candidate = assign_town_by_nearest_parcel(candidate, parcels)

    print("Assigning towns to LPA set")
    lpa = assign_town_by_nearest_parcel(lpa, parcels)

    print("Building empirical town prior")
    town_prior = build_town_prior(candidate, lpa)

    print("Filtering background pool away from existing LPAs")
    candidate_bg_pool = filter_background_away_from_lpa(candidate, lpa)

    print("Generating background")
    background = generate_background(candidate_bg_pool, len(lpa))

    print("Building training feature dataset")
    training_features = pd.concat([lpa, background], ignore_index=True)
    training_features = gpd.GeoDataFrame(training_features, geometry="geometry", crs=TARGET_CRS)

    print("Computing training features")
    training_features = compute_features(training_features, parcels, shoreline, leases)
    training_features = attach_town_prior(training_features, town_prior)
    training_features = add_log_features(training_features)

    return training_features, town_prior


def build_training_matrix(training_features: gpd.GeoDataFrame) -> pd.DataFrame:
    cols = ["decision"] + FEATURES + ["OBJECTID"]
    cols = [c for c in cols if c in training_features.columns]

    training_matrix = training_features[cols].copy()
    training_matrix = training_matrix.replace([np.inf, -np.inf], np.nan)
    training_matrix = training_matrix.dropna(subset=["decision"] + FEATURES)
    return training_matrix


def build_candidate_scoring_features(
    candidate: gpd.GeoDataFrame,
    parcels: gpd.GeoDataFrame,
    shoreline: gpd.GeoDataFrame,
    leases: gpd.GeoDataFrame,
    town_prior: pd.DataFrame,
) -> gpd.GeoDataFrame:
    print("Scoring candidate sites")
    candidate_scored = compute_features(candidate, parcels, shoreline, leases)
    candidate_scored = attach_town_prior(candidate_scored, town_prior)
    candidate_scored = add_log_features(candidate_scored)
    return candidate_scored


# -----------------------------------------------------
# Common loaders for model scripts
# -----------------------------------------------------

def load_candidate_sites(candidate_path: str | Path) -> gpd.GeoDataFrame:
    candidate = gpd.read_file(candidate_path).to_crs(TARGET_CRS)
    candidate = clean_geometries(candidate)

    candidate_access_scores = load_candidate_access_scores()
    candidate = candidate.merge(
        candidate_access_scores,
        how="left",
        on="candidate_id",
    )
    return candidate


def load_lpa_sites_with_access() -> gpd.GeoDataFrame:
    lpa = download_lpa_sites()
    lpa_access_scores = load_lpa_access_scores()
    lpa = lpa.merge(
        lpa_access_scores,
        how="left",
        on="OBJECTID",
    )
    return lpa