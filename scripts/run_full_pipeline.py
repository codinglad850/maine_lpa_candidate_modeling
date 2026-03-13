#!/usr/bin/env python3

from __future__ import annotations

import subprocess
import sys
import time

from src.paths import (
    CANDIDATES_DEPTH_MLLW,
    CANDIDATES_DEPTH_MLLW_SCHEMA,
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
    EMPIRICAL_MODEL_FEATURES,
    EMPIRICAL_MODEL_FEATURES_SCHEMA,
    EMPIRICAL_MODEL_TRAINING_MATRIX,
    EMPIRICAL_MODEL_TRAINING_MATRIX_SCHEMA,
    CANDIDATES_SCORED,
    CANDIDATE_SPOTS_SCORED_SCHEMA,
)

from src.validation import validate_geofile, validate_table_file



STAGES = [

    # -------------------------------------------------
    # Depth → MLLW
    # -------------------------------------------------

    {
        "label": "Convert NAVD88 → MLLW",
        "module": "scripts.convert_candidates_navd88_to_mllw",
        "checks": [
            ("table", CANDIDATES_DEPTH_MLLW, CANDIDATES_DEPTH_MLLW_SCHEMA),
        ],
    },

    # -------------------------------------------------
    # Intertidal selection
    # -------------------------------------------------

    {
        "label": "Build intertidal candidates",
        "module": "scripts.build_intertidal_candidates",
        "checks": [
            ("geo", CANDIDATES_INTERTIDAL, CANDIDATES_INTERTIDAL_SCHEMA),
        ],
    },

    # -------------------------------------------------
    # Access normalization
    # -------------------------------------------------

    {
        "label": "Build normalized access points",
        "module": "scripts.build_normalized_access_points",
        "checks": [
            ("geo", NORMALIZED_ACCESS_POINTS, NORMALIZED_ACCESS_POINTS_SCHEMA),
        ],
    },

    # -------------------------------------------------
    # Access scoring
    # -------------------------------------------------

    {
        "label": "Score site access",
        "module": "scripts.score_lpa_candidate_access",
        "checks": [
            ("geo", CANDIDATE_ACCESS_SCORED, CANDIDATE_ACCESS_SCORED_SCHEMA),
            ("geo", LPA_ACCESS_SCORED, LPA_ACCESS_SCORED_SCHEMA),
        ],
    },

    # -------------------------------------------------
    # Feature build
    # -------------------------------------------------

    {
        "label": "Build empirical model features",
        "module": "scripts.build_empirical_model_features",
        "checks": [
            ("table", TOWN_PRIOR_CSV, TOWN_PRIOR_SCHEMA),
            ("table", EMPIRICAL_MODEL_FEATURES, EMPIRICAL_MODEL_FEATURES_SCHEMA),
            ("table", EMPIRICAL_MODEL_TRAINING_MATRIX, EMPIRICAL_MODEL_TRAINING_MATRIX_SCHEMA),
        ],
    },

    # -------------------------------------------------
    # Train model
    # -------------------------------------------------

    {
        "label": "Train empirical model",
        "module": "scripts.train_empirical_model",
        "checks": [
            # no schema validation for pickle yet
        ],
    },

    # -------------------------------------------------
    # Score candidates
    # -------------------------------------------------

    {
        "label": "Score candidates with empirical model",
        "module": "scripts.score_candidates_empirical_model",
        "checks": [
            ("geo", CANDIDATES_SCORED, CANDIDATE_SPOTS_SCORED_SCHEMA),
        ],
    },
]

def run_stage(label: str, module_name: str) -> None:
    print("\n" + "=" * 80)
    print(f"START: {label}")
    print(f"MODULE: {module_name}")
    print("=" * 80)

    t0 = time.time()

    result = subprocess.run(
        [sys.executable, "-m", module_name],
        check=False,
    )

    dt = time.time() - t0

    if result.returncode != 0:
        raise RuntimeError(
            f"Stage failed: {label}\n"
            f"Module: {module_name}\n"
            f"Exit code: {result.returncode}\n"
            f"Elapsed seconds: {dt:.1f}"
        )

    print(f"DONE: {label} ({dt:.1f}s)")


def validate_stage_outputs(label, checks):

    if not checks:
        print(f"No validation checks configured for: {label}")
        return

    print(f"Validating outputs for: {label}")

    for kind, artifact_path, schema_path in checks:

        print(f"  -> {artifact_path}")

        if kind == "geo":
            validate_geofile(artifact_path, schema_path)

        elif kind == "table":
            validate_table_file(artifact_path, schema_path)

        else:
            raise ValueError(f"Unknown validation kind: {kind}")

    print(f"Validation passed for: {label}")


def main():

    pipeline_start = time.time()

    for stage in STAGES:

        run_stage(stage["label"], stage["module"])

        validate_stage_outputs(
            stage["label"],
            stage["checks"],
        )

    total_dt = time.time() - pipeline_start

    print("\n" + "=" * 80)
    print(f"PIPELINE COMPLETE ({total_dt:.1f}s)")
    print("=" * 80)


if __name__ == "__main__":
    main()