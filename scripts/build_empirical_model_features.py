#!/usr/bin/env python3

from __future__ import annotations

from src.paths import (
    CANDIDATES_INTERTIDAL,
    EMPIRICAL_MODEL_FEATURES,
    EMPIRICAL_MODEL_TRAINING_MATRIX,
    TOWN_PRIOR_CSV,
    EMPIRICAL_MODEL_FEATURES_SCHEMA,
    EMPIRICAL_MODEL_TRAINING_MATRIX_SCHEMA,
    TOWN_PRIOR_SCHEMA,
)
from src.modeling.empirical_features import (
    load_candidate_sites,
    load_lpa_sites_with_access,
    load_shoreline,
    load_parcels,
    download_leases,
    build_training_features,
    build_training_matrix,
)
from src.validation import validate_table_file

# assembles the features that will be used for modeling LPA acceptance probability

def ensure_parent(path):
    path.parent.mkdir(parents=True, exist_ok=True)


def main() -> None:
    print("Loading canonical/intermediate inputs")
    candidate = load_candidate_sites(CANDIDATES_INTERTIDAL)
    shoreline = load_shoreline()
    parcels = load_parcels()
    lpa = load_lpa_sites_with_access()
    leases = download_leases()

    print("Building empirical model training features")
    training_features, town_prior = build_training_features(
        candidate=candidate,
        parcels=parcels,
        shoreline=shoreline,
        lpa=lpa,
        leases=leases,
    )

    print("Writing town prior")
    ensure_parent(TOWN_PRIOR_CSV)
    town_prior.to_csv(TOWN_PRIOR_CSV, index=False)

    print("Writing full empirical feature table")
    feature_export = (
        training_features
        .drop(columns="geometry", errors="ignore")
        .replace([float("inf"), float("-inf")], float("nan"))
    )
    ensure_parent(EMPIRICAL_MODEL_FEATURES)
    feature_export.to_parquet(EMPIRICAL_MODEL_FEATURES, index=False)

    print("Building final training matrix")
    training_matrix = build_training_matrix(training_features)

    print("Writing training matrix")
    ensure_parent(EMPIRICAL_MODEL_TRAINING_MATRIX)
    training_matrix.to_parquet(EMPIRICAL_MODEL_TRAINING_MATRIX, index=False)

    print("Validating outputs")
    validate_table_file(TOWN_PRIOR_CSV, TOWN_PRIOR_SCHEMA)
    validate_table_file(EMPIRICAL_MODEL_FEATURES, EMPIRICAL_MODEL_FEATURES_SCHEMA)
    validate_table_file(
        EMPIRICAL_MODEL_TRAINING_MATRIX,
        EMPIRICAL_MODEL_TRAINING_MATRIX_SCHEMA,
    )

    print(f"Wrote {TOWN_PRIOR_CSV}")
    print(f"Wrote {EMPIRICAL_MODEL_FEATURES}")
    print(f"Wrote {EMPIRICAL_MODEL_TRAINING_MATRIX}")
    print("Empirical model feature-build stage complete")


if __name__ == "__main__":
    main()