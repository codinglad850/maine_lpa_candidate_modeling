from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd
import pytest
from shapely.geometry import LineString, Polygon

from src.sources.common import LocalSource, RemoteSource, source_dict
import scripts.maine_hard_constraint_mask as mod


WGS84 = "EPSG:4326"
METRIC_CRS = "EPSG:26919"


def _write_geojson(path: Path, gdf: gpd.GeoDataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(path, driver="GeoJSON")


def _snapshot_geojson_path(
    snapshots_dir: Path,
    snapshot_date: str,
    group: str,
    key: str,
) -> Path:
    return snapshots_dir / snapshot_date / group / f"{key}.geojson"


def _round_bounds(bounds, ndigits=6):
    return [round(float(v), ndigits) for v in bounds]


def _compute_mask_metrics(gdf: gpd.GeoDataFrame) -> dict:
    if gdf.crs is None:
        raise ValueError("Output GeoDataFrame has no CRS")

    gdf_wgs84 = gdf.to_crs(WGS84)
    gdf_metric = gdf.to_crs(METRIC_CRS)

    union_geom = gdf_metric.geometry.union_all()
    union_area_m2 = float(union_geom.area)

    metrics = {
        "feature_count": int(len(gdf_wgs84)),
        "geometry_types": sorted(gdf_wgs84.geom_type.unique().tolist()),
        "bounds_4326": _round_bounds(gdf_wgs84.total_bounds, 6),
        "union_area_m2": round(union_area_m2, 3),
    }
    return metrics


def _build_regression_fixture(
    tmp_path: Path,
    snapshot_date: str = "2026-01-01",
) -> tuple[Path, Path]:
    """
    Build a tiny but realistic fixture set:
    - one local polygon mask
    - one remote polygon snapshot
    - one remote nav line snapshot (to be buffered)
    Returns (snapshots_dir, local_mask_path).
    """
    snapshots_dir = tmp_path / "snapshots"
    local_dir = tmp_path / "local"
    local_dir.mkdir(parents=True, exist_ok=True)

    # Local polygon
    local_mask_path = local_dir / "local_mask.geojson"
    local_gdf = gpd.GeoDataFrame(
        {"name": ["local_mask"]},
        geometry=[
            Polygon(
                [
                    (-70.10, 43.00),
                    (-70.00, 43.00),
                    (-70.00, 43.10),
                    (-70.10, 43.10),
                ]
            )
        ],
        crs=WGS84,
    )
    _write_geojson(local_mask_path, local_gdf)

    # Remote polygon snapshot
    polygon_src_key = "remote_polygon"
    polygon_gdf = gpd.GeoDataFrame(
        {"name": ["remote_polygon"]},
        geometry=[
            Polygon(
                [
                    (-69.95, 43.00),
                    (-69.85, 43.00),
                    (-69.85, 43.10),
                    (-69.95, 43.10),
                ]
            )
        ],
        crs=WGS84,
    )
    _write_geojson(
        _snapshot_geojson_path(snapshots_dir, snapshot_date, "constraints", polygon_src_key),
        polygon_gdf,
    )

    # Remote nav line snapshot (will be buffered)
    nav_src_key = "remote_nav"
    nav_gdf = gpd.GeoDataFrame(
        {"name": ["remote_nav"]},
        geometry=[
            LineString(
                [
                    (-69.80, 43.00),
                    (-69.75, 43.05),
                ]
            )
        ],
        crs=WGS84,
    )
    _write_geojson(
        _snapshot_geojson_path(snapshots_dir, snapshot_date, "constraints", nav_src_key),
        nav_gdf,
    )

    return snapshots_dir, local_mask_path


def test_hard_constraint_mask_regression_against_expected_metrics(
    monkeypatch,
    tmp_path: Path,
) -> None:
    snapshot_date = "2026-01-01"
    snapshots_dir, local_mask_path = _build_regression_fixture(
        tmp_path,
        snapshot_date=snapshot_date,
    )

    # Small fake source catalog for deterministic regression
    local_src = LocalSource(
        key="local_mask",
        name="Local Mask",
        kind="local_file",
        path=str(local_mask_path),
        purpose="Regression test local mask",
        expected_geometry="Polygon",
        expected_crs="EPSG:4326",
        tags=("test",),
    )

    polygon_src = RemoteSource(
        key="remote_polygon",
        name="Remote Polygon",
        kind="arcgis_featureserver",
        url="https://example.com/polygon",
        purpose="Regression test remote polygon",
        expected_geometry="Polygon",
        expected_crs="EPSG:4326",
        tags=("test",),
    )

    nav_src = RemoteSource(
        key="remote_nav",
        name="Remote Nav",
        kind="arcgis_featureserver",
        url="https://example.com/nav",
        purpose="Regression test nav line",
        expected_geometry="LineString",
        expected_crs="EPSG:4326",
        tags=("test",),
    )

    monkeypatch.setattr(
        mod,
        "HARD_CONSTRAINT_LOCAL_SOURCES",
        source_dict(local_src),
    )
    monkeypatch.setattr(mod, "CONSTRAINT_POLYGON_SOURCES", [polygon_src])
    monkeypatch.setattr(mod, "NOAA_NAV_SOURCES", [nav_src])
    monkeypatch.setattr(mod, "AQ_SOURCES", [])
    monkeypatch.setattr(mod, "INCLUDE_AQUACULTURE", False)

    out = mod.build_hard_constraint_mask(
        snapshot_date=snapshot_date,
        snapshots_dir=snapshots_dir,
        include_aquaculture=False,
    )

    actual = _compute_mask_metrics(out)

    # This is the "golden" expected result for the fixed fixture data above.
    # If the generation logic changes intentionally, update these values carefully.
    expected = {
        "feature_count": 3,
        "geometry_types": ["Polygon"],
        "bounds_4326": [-70.1, 42.997258, -69.746261, 43.1],
        "union_area_m2": 185399850.095,
    }

    def assert_bounds_close(a, b, tol=1e-5):
        assert len(a) == len(b)
        for i, (x, y) in enumerate(zip(a, b)):
            assert abs(x - y) < tol, f"bounds[{i}] differs: {x} vs {y}"

    def assert_close(a, b, rel=1e-6, abs_tol=1.0):
        diff = abs(a - b)
        allowed = max(abs_tol, rel * abs(b))
        assert diff < allowed, f"{a} vs {b} diff={diff} allowed={allowed}"

    assert actual["feature_count"] == expected["feature_count"]

    assert actual["geometry_types"] == expected["geometry_types"]

    assert_bounds_close(
        actual["bounds_4326"],
        expected["bounds_4326"],
        tol=1e-5, # around here, 1 degree is on the order of 100k m
    )
    print(actual["union_area_m2"])
    print(expected["union_area_m2"])

    assert_close(
        actual["union_area_m2"],
        expected["union_area_m2"],
        rel=1e-6,
        abs_tol=1.0,
    )


def test_hard_constraint_mask_regression_metrics_can_be_saved(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """
    Utility-style test showing how to write an expected metrics file for a fixture.
    This is useful when first establishing a regression baseline.
    """
    snapshot_date = "2026-01-01"
    snapshots_dir, local_mask_path = _build_regression_fixture(
        tmp_path,
        snapshot_date=snapshot_date,
    )

    local_src = LocalSource(
        key="local_mask",
        name="Local Mask",
        kind="local_file",
        path=str(local_mask_path),
        purpose="Regression test local mask",
        expected_geometry="Polygon",
        expected_crs="EPSG:4326",
        tags=("test",),
    )

    polygon_src = RemoteSource(
        key="remote_polygon",
        name="Remote Polygon",
        kind="arcgis_featureserver",
        url="https://example.com/polygon",
        purpose="Regression test remote polygon",
        expected_geometry="Polygon",
        expected_crs="EPSG:4326",
        tags=("test",),
    )

    nav_src = RemoteSource(
        key="remote_nav",
        name="Remote Nav",
        kind="arcgis_featureserver",
        url="https://example.com/nav",
        purpose="Regression test nav line",
        expected_geometry="LineString",
        expected_crs="EPSG:4326",
        tags=("test",),
    )

    monkeypatch.setattr(
        mod,
        "HARD_CONSTRAINT_LOCAL_SOURCES",
        source_dict(local_src),
    )
    monkeypatch.setattr(mod, "CONSTRAINT_POLYGON_SOURCES", [polygon_src])
    monkeypatch.setattr(mod, "NOAA_NAV_SOURCES", [nav_src])
    monkeypatch.setattr(mod, "AQ_SOURCES", [])
    monkeypatch.setattr(mod, "INCLUDE_AQUACULTURE", False)

    out = mod.build_hard_constraint_mask(
        snapshot_date=snapshot_date,
        snapshots_dir=snapshots_dir,
        include_aquaculture=False,
    )

    metrics = _compute_mask_metrics(out)

    metrics_path = tmp_path / "expected_metrics.json"
    with open(metrics_path, "w") as f:
        json.dump(metrics, f, indent=2)

    assert metrics_path.exists()
    reloaded = json.loads(metrics_path.read_text())
    assert reloaded["feature_count"] == metrics["feature_count"]