#!/usr/bin/env python3
from __future__ import annotations

from src.paths import (
    CANDIDATES_INTERTIDAL,
    CANDIDATES_INTERTIDAL_SCHEMA,
    NORMALIZED_ACCESS_POINTS,
    NORMALIZED_ACCESS_POINTS_SCHEMA,
    CANDIDATE_ACCESS_SCORED,
    CANDIDATE_ACCESS_SCORED_SCHEMA,
    LPA_ACCESS_SCORED,
    LPA_ACCESS_SCORED_SCHEMA,
    TOWN_PRIOR_CSV,
    TOWN_PRIOR_SCHEMA,
    EMPIRICAL_MODEL_TRAINING_MATRIX,
    EMPIRICAL_MODEL_TRAINING_MATRIX_SCHEMA,
)
from src.validation import validate_geofile, validate_table_file


CHECKS = [
    ("candidates_intertidal", CANDIDATES_INTERTIDAL, CANDIDATES_INTERTIDAL_SCHEMA, "geo"),
    ("normalized_access_points", NORMALIZED_ACCESS_POINTS, NORMALIZED_ACCESS_POINTS_SCHEMA, "geo"),
    ("candidate_access_scored", CANDIDATE_ACCESS_SCORED, CANDIDATE_ACCESS_SCORED_SCHEMA, "geo"),
    ("lpa_access_scored", LPA_ACCESS_SCORED, LPA_ACCESS_SCORED_SCHEMA, "geo"),
    ("town_prior", TOWN_PRIOR_CSV, TOWN_PRIOR_SCHEMA, "table"),
    ("empirical_model_training_matrix", EMPIRICAL_MODEL_TRAINING_MATRIX, EMPIRICAL_MODEL_TRAINING_MATRIX_SCHEMA, "table"),
]


def main() -> None:
    failed = []

    for name, artifact_path, schema_path, kind in CHECKS:
        try:
            print(f"Validating {name}...")
            if kind == "geo":
                validate_geofile(artifact_path, schema_path)
            elif kind == "table":
                validate_table_file(artifact_path, schema_path)
            else:
                raise ValueError(f"Unknown check kind: {kind}")
            print(f"  PASS: {name}")
        except Exception as e:
            print(f"  FAIL: {name} -> {e}")
            failed.append((name, str(e)))

    if failed:
        print("\nValidation failed:")
        for name, err in failed:
            print(f"- {name}: {err}")
        raise SystemExit(1)

    print("\nAll validations passed.")


if __name__ == "__main__":
    main()