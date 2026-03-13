#!/usr/bin/env python3

from __future__ import annotations

import json
from datetime import datetime, UTC

import joblib
import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import train_test_split

from src.paths import (
    EMPIRICAL_MODEL_TRAINING_MATRIX,
    EMPIRICAL_MODEL_PKL,
    EMPIRICAL_MODEL_METADATA,
    EMPIRICAL_MODEL_TRAINING_MATRIX_SCHEMA,
)
from src.validation import validate_table_file
from src.modeling.empirical_features import FEATURES


def ensure_parent(path):
    path.parent.mkdir(parents=True, exist_ok=True)


# -----------------------------------------------------
# Train model
# -----------------------------------------------------

def train_model(df: pd.DataFrame):

    X = df[FEATURES]
    y = df["decision"]

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=42,
        stratify=y,
    )

    model = GradientBoostingClassifier(
        n_estimators=200,
        learning_rate=0.05,
        max_depth=3,
        min_samples_leaf=20,
        random_state=42,
    )

    model.fit(X_train, y_train)

    acc = model.score(X_test, y_test)
    print("Model accuracy:", acc)

    importance = (
        pd.Series(model.feature_importances_, index=FEATURES)
        .sort_values(ascending=False)
    )

    print("\nFeature importance:")
    print(importance.to_string())

    return model, acc


# -----------------------------------------------------
# Metadata
# -----------------------------------------------------

def build_metadata(df: pd.DataFrame, accuracy: float):

    return {
        "created_at_utc": datetime.now(UTC).isoformat(),
        "model_class": "GradientBoostingClassifier",
        "feature_columns": FEATURES,
        "target_column": "decision",
        "n_training_rows": int(len(df)),
        "n_training_columns": int(df.shape[1]),
        "accuracy": float(accuracy),
        "artifacts": {
            "training_matrix": str(EMPIRICAL_MODEL_TRAINING_MATRIX),
            "model_pickle": str(EMPIRICAL_MODEL_PKL),
        },
        "parameters": {
            "n_estimators": 200,
            "learning_rate": 0.05,
            "max_depth": 3,
            "min_samples_leaf": 20,
            "random_state": 42,
        },
    }


# -----------------------------------------------------
# Main
# -----------------------------------------------------

def main():

    print("Validating training matrix")
    validate_table_file(
        EMPIRICAL_MODEL_TRAINING_MATRIX,
        EMPIRICAL_MODEL_TRAINING_MATRIX_SCHEMA,
    )

    print("Loading training matrix")
    df = pd.read_parquet(EMPIRICAL_MODEL_TRAINING_MATRIX)

    print("Training empirical model")
    model, acc = train_model(df)

    print("Saving model")
    ensure_parent(EMPIRICAL_MODEL_PKL)
    joblib.dump(model, EMPIRICAL_MODEL_PKL)

    print("Writing metadata")
    metadata = build_metadata(df, acc)

    ensure_parent(EMPIRICAL_MODEL_METADATA)
    with open(EMPIRICAL_MODEL_METADATA, "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"Wrote {EMPIRICAL_MODEL_PKL}")
    print(f"Wrote {EMPIRICAL_MODEL_METADATA}")
    print("Empirical model training complete")


if __name__ == "__main__":
    main()