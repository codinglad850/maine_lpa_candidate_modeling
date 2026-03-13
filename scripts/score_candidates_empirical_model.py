#!/usr/bin/env python3

from __future__ import annotations

import joblib
import numpy as np
import pandas as pd

from src.paths import (
    CANDIDATES_INTERTIDAL,
    TOWN_PRIOR_CSV,
    EMPIRICAL_MODEL_PKL,
    CANDIDATES_SCORED,
    CANDIDATES_SCORED_CSV,
    CANDIDATE_SPOTS_SCORED_SCHEMA,
)
from src.modeling.empirical_features import (
    FEATURES,
    WGS84,
    load_candidate_sites,
    load_shoreline,
    load_parcels,
    download_leases,
    build_candidate_scoring_features,
)
from src.validation import validate_geofile


def ensure_parent(path):
    path.parent.mkdir(parents=True, exist_ok=True)


def main() -> None:
    print("Loading candidate sites")
    candidate = load_candidate_sites(CANDIDATES_INTERTIDAL)

    print("Loading supporting data")
    shoreline = load_shoreline()
    parcels = load_parcels()
    leases = download_leases()

    print("Loading town prior")
    town_prior = pd.read_csv(TOWN_PRIOR_CSV)

    print("Loading trained model")
    model = joblib.load(EMPIRICAL_MODEL_PKL)

    print("Building candidate scoring features")
    candidate_scored = build_candidate_scoring_features(
        candidate=candidate,
        parcels=parcels,
        shoreline=shoreline,
        leases=leases,
        town_prior=town_prior,
    )

    print("Scoring candidates")
    X = candidate_scored[FEATURES]
    candidate_scored["approval_probability"] = model.predict_proba(X)[:, 1]

    export_cols: list[str] = []

    if "town" in candidate_scored.columns:
        export_cols.append("town")

    for col in [
        "approval_probability",
        "town_logit_prior",
        "access_score",
        "d_access_m",
        "homes_500m",
        "homes_1000m",
        "mean_land_value",
        "distance_to_shore",
        "dist_lease_cluster",
        "log_mean_land_value",
        "log_distance_to_shore",
        "log_dist_lease_cluster",
    ]:
        if col in candidate_scored.columns and col not in export_cols:
            export_cols.append(col)

    original_cols = [
        c for c in candidate.columns
        if c != "geometry" and c not in export_cols
    ]
    export_cols = original_cols + export_cols + ["geometry"]

    candidate_export = candidate_scored[export_cols].copy()
    candidate_export = candidate_export.replace([np.inf, -np.inf], np.nan)
    candidate_export = candidate_export.fillna(0)

    print("Writing scored GeoJSON")
    candidate_export_geojson = candidate_export.to_crs(WGS84)
    ensure_parent(CANDIDATES_SCORED)
    candidate_export_geojson.to_file(CANDIDATES_SCORED, driver="GeoJSON")

    print("Validating scored GeoJSON")
    validate_geofile(CANDIDATES_SCORED, CANDIDATE_SPOTS_SCORED_SCHEMA)

    print("Writing scored CSV")
    candidate_csv = candidate_export_geojson.copy()
    candidate_csv["lon"] = candidate_csv.geometry.x
    candidate_csv["lat"] = candidate_csv.geometry.y

    ensure_parent(CANDIDATES_SCORED_CSV)
    (
        candidate_csv
        .drop(columns="geometry")
        .sort_values("approval_probability", ascending=False)
        .to_csv(CANDIDATES_SCORED_CSV, index=False)
    )

    print(f"Wrote {CANDIDATES_SCORED}")
    print(f"Wrote {CANDIDATES_SCORED_CSV}")
    print("Candidate scoring complete")


if __name__ == "__main__":
    main()