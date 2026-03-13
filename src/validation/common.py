from __future__ import annotations

from pathlib import Path
from typing import Any

import geopandas as gpd
import pandas as pd
import yaml


def load_schema(path: str | Path) -> dict[str, Any]:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def _required_fields(schema: dict[str, Any]) -> list[str]:
    if "required_fields" in schema:
        return list(schema["required_fields"])

    fields = schema.get("fields", [])
    return [f["name"] for f in fields if f.get("required", False)]


def _field_defs(schema: dict[str, Any]) -> dict[str, dict[str, Any]]:
    defs: dict[str, dict[str, Any]] = {}

    for section in ("fields", "recommended_fields"):
        entries = schema.get(section, []) or []
        for f in entries:
            if isinstance(f, str):
                defs.setdefault(f, {})
            elif isinstance(f, dict):
                if "name" not in f:
                    raise ValueError(f"Field spec in {section} missing 'name': {f}")
                defs[f["name"]] = f
            else:
                raise TypeError(
                    f"Unsupported field spec type in {section}: {type(f)}"
                )

    # also merge simple type map if present
    for name, typ in (schema.get("types") or {}).items():
        defs.setdefault(name, {})
        defs[name]["type"] = typ

    return defs


def _primary_key(schema: dict[str, Any]) -> str | None:
    pk = schema.get("primary_key")
    if pk in (None, "null"):
        return None
    return pk


def _check_required_columns(df: pd.DataFrame, schema: dict[str, Any]) -> None:
    required = _required_fields(schema)
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def _check_primary_key(df: pd.DataFrame, schema: dict[str, Any]) -> None:
    pk = _primary_key(schema)
    if not pk:
        return

    if pk not in df.columns:
        raise ValueError(f"Primary key column not found: {pk}")

    constraints = schema.get("constraints", {}) or {}

    if constraints.get("primary_key_not_null", False):
        if df[pk].isna().any():
            raise ValueError(f"Null values found in primary key: {pk}")

    if constraints.get("primary_key_unique", False):
        if df[pk].duplicated().any():
            dupes = df.loc[df[pk].duplicated(), pk].tolist()[:10]
            raise ValueError(f"Duplicate primary key values in {pk}: {dupes}")


def _check_min_rows(df: pd.DataFrame, schema: dict[str, Any]) -> None:
    constraints = schema.get("constraints", {}) or {}
    min_rows = constraints.get("min_rows")
    if min_rows is not None and len(df) < min_rows:
        raise ValueError(f"Expected at least {min_rows} rows, found {len(df)}")


def _check_no_nulls_in(df: pd.DataFrame, schema: dict[str, Any]) -> None:
    constraints = schema.get("constraints", {}) or {}
    cols = constraints.get("no_nulls_in", []) or []
    for col in cols:
        if col not in df.columns:
            raise ValueError(f"Column in no_nulls_in not found: {col}")
        if df[col].isna().any():
            raise ValueError(f"Null values found in required non-null column: {col}")


def _check_field_constraints(df: pd.DataFrame, schema: dict[str, Any]) -> None:
    defs = _field_defs(schema)

    for col, spec in defs.items():
        if col not in df.columns:
            continue

        s = df[col]

        if spec.get("nullable") is False and s.isna().any():
            raise ValueError(f"Null values found in non-nullable column: {col}")

        if "allowed_values" in spec:
            allowed = set(spec["allowed_values"])
            bad = s.dropna()[~s.dropna().isin(allowed)]
            if len(bad) > 0:
                vals = sorted(pd.Series(bad).astype(str).unique().tolist())[:10]
                raise ValueError(f"Invalid values in {col}: {vals}")

        if spec.get("type") in ("float", "integer"):
            if "min" in spec:
                bad = s.dropna() < spec["min"]
                if bad.any():
                    raise ValueError(f"Values below min found in {col}")
            if "max" in spec:
                bad = s.dropna() > spec["max"]
                if bad.any():
                    raise ValueError(f"Values above max found in {col}")


def _normalize_crs_string(crs: Any) -> str | None:
    if crs is None:
        return None
    try:
        return crs.to_string()
    except Exception:
        return str(crs)


def _check_geometry(gdf: gpd.GeoDataFrame, schema: dict[str, Any]) -> None:
    geom_spec = schema.get("geometry")
    if not geom_spec:
        return

    if gdf.geometry is None:
        raise ValueError("GeoDataFrame has no active geometry column")

    if geom_spec.get("required", False) and gdf.geometry.isna().any():
        raise ValueError("Null geometries found")

    if geom_spec.get("allow_empty") is False and gdf.geometry.is_empty.any():
        raise ValueError("Empty geometries found")

    allowed_geom_types = []
    if "allowed_geometry_types" in (schema.get("constraints") or {}):
        allowed_geom_types = (schema.get("constraints") or {}).get("allowed_geometry_types") or []
    elif "type" in geom_spec:
        allowed_geom_types = [geom_spec["type"]]

    if allowed_geom_types:
        bad = ~gdf.geom_type.isin(allowed_geom_types)
        if bad.any():
            bad_types = sorted(gdf.loc[bad].geom_type.unique().tolist())
            raise ValueError(f"Invalid geometry types found: {bad_types}")

    crs_allowed = geom_spec.get("crs_allowed")
    if crs_allowed:
        actual = _normalize_crs_string(gdf.crs)
        normalized_allowed = set(crs_allowed) | {"urn:ogc:def:crs:OGC:1.3:CRS84"}
        if actual not in normalized_allowed:
            raise ValueError(f"CRS {actual} not in allowed set {sorted(normalized_allowed)}")

    # optional lon/lat consistency check
    if "lon" in gdf.columns and "lat" in gdf.columns:
        tol = 1e-9
        xdiff = (gdf.geometry.x - gdf["lon"]).abs()
        ydiff = (gdf.geometry.y - gdf["lat"]).abs()
        if (xdiff > tol).any() or (ydiff > tol).any():
            raise ValueError("lon/lat columns do not match geometry coordinates")


def validate_dataframe(df: pd.DataFrame, schema: dict[str, Any]) -> None:
    _check_required_columns(df, schema)
    _check_primary_key(df, schema)
    _check_min_rows(df, schema)
    _check_no_nulls_in(df, schema)
    _check_field_constraints(df, schema)


def validate_geodataframe(gdf: gpd.GeoDataFrame, schema: dict[str, Any]) -> None:
    validate_dataframe(gdf, schema)
    _check_geometry(gdf, schema)


def validate_table_file(path: str | Path, schema_path: str | Path) -> pd.DataFrame:
    schema = load_schema(schema_path)
    path = Path(path)

    if path.suffix.lower() == ".csv":
        df = pd.read_csv(path)
    elif path.suffix.lower() == ".parquet":
        df = pd.read_parquet(path)
    else:
        raise ValueError(f"Unsupported tabular format for validation: {path}")

    validate_dataframe(df, schema)
    return df


def validate_geofile(path: str | Path, schema_path: str | Path) -> gpd.GeoDataFrame:
    schema = load_schema(schema_path)
    gdf = gpd.read_file(path)
    validate_geodataframe(gdf, schema)
    return gdf