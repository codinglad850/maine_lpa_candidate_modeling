from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pytest
from shapely.geometry import LineString, Polygon

from src.sources.common import LocalSource, RemoteSource, source_dict
import scripts.maine_hard_constraint_mask as mod


@pytest.fixture
def snapshot_date() -> str:
    return "2026-01-01"


@pytest.fixture
def snapshots_dir(tmp_path: Path) -> Path:
    d = tmp_path / "snapshots"
    d.mkdir(parents=True, exist_ok=True)
    return d


@pytest.fixture
def local_data_dir(tmp_path: Path) -> Path:
    d = tmp_path / "local"
    d.mkdir(parents=True, exist_ok=True)
    return d


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


def test_build_hard_constraint_mask_from_snapshots(
    monkeypatch,
    snapshots_dir: Path,
    local_data_dir: Path,
    snapshot_date: str,
) -> None:
    # ------------------------------------------------------------------
    # Local polygon mask
    # ------------------------------------------------------------------
    local_mask_path = local_data_dir / "local_mask.geojson"
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
        crs="EPSG:4326",
    )
    _write_geojson(local_mask_path, local_gdf)

    local_src = LocalSource(
        key="local_mask",
        name="Local Mask",
        kind="local_file",
        path=str(local_mask_path),
        purpose="Test local mask",
        expected_geometry="Polygon",
        expected_crs="EPSG:4326",
        tags=("test",),
    )

    # ------------------------------------------------------------------
    # Remote polygon snapshot
    # ------------------------------------------------------------------
    polygon_src = RemoteSource(
        key="remote_polygon",
        name="Remote Polygon",
        kind="arcgis_featureserver",
        url="https://example.com/polygon",
        purpose="Test remote polygon",
        expected_geometry="Polygon",
        expected_crs="EPSG:4326",
        tags=("test",),
    )

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
        crs="EPSG:4326",
    )
    _write_geojson(
        _snapshot_geojson_path(snapshots_dir, snapshot_date, "constraints", polygon_src.key),
        polygon_gdf,
    )

    # ------------------------------------------------------------------
    # Remote NOAA-style line snapshot (will be buffered)
    # ------------------------------------------------------------------
    nav_src = RemoteSource(
        key="remote_nav",
        name="Remote Nav",
        kind="arcgis_featureserver",
        url="https://example.com/nav",
        purpose="Test remote nav line",
        expected_geometry="LineString",
        expected_crs="EPSG:4326",
        tags=("test",),
    )

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
        crs="EPSG:4326",
    )
    _write_geojson(
        _snapshot_geojson_path(snapshots_dir, snapshot_date, "constraints", nav_src.key),
        nav_gdf,
    )

    # ------------------------------------------------------------------
    # Monkeypatch source catalogs so the test is tiny and isolated
    # ------------------------------------------------------------------
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

    assert isinstance(out, gpd.GeoDataFrame)
    assert len(out) > 0
    assert out.crs is not None
    assert str(out.crs).upper() in {"EPSG:4326", "OGC:CRS84", "WGS 84"}

    # Buffered nav lines should become polygons, so output should be polygonal.
    geom_types = set(out.geom_type.unique().tolist())
    assert geom_types.issubset({"Polygon", "MultiPolygon"})

    # We expect at least one local polygon, one remote polygon, and one buffered nav output.
    assert len(out) >= 3


def test_write_hard_constraint_mask_writes_gpkg(
    monkeypatch,
    snapshots_dir: Path,
    local_data_dir: Path,
    snapshot_date: str,
    tmp_path: Path,
) -> None:
    # Minimal local mask
    local_mask_path = local_data_dir / "local_mask.geojson"
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
        crs="EPSG:4326",
    )
    _write_geojson(local_mask_path, local_gdf)

    local_src = LocalSource(
        key="local_mask",
        name="Local Mask",
        kind="local_file",
        path=str(local_mask_path),
        purpose="Test local mask",
        expected_geometry="Polygon",
        expected_crs="EPSG:4326",
        tags=("test",),
    )

    monkeypatch.setattr(
        mod,
        "HARD_CONSTRAINT_LOCAL_SOURCES",
        source_dict(local_src),
    )
    monkeypatch.setattr(mod, "CONSTRAINT_POLYGON_SOURCES", [])
    monkeypatch.setattr(mod, "NOAA_NAV_SOURCES", [])
    monkeypatch.setattr(mod, "AQ_SOURCES", [])
    monkeypatch.setattr(mod, "INCLUDE_AQUACULTURE", False)

    out_path = tmp_path / "combined_mask.gpkg"

    written = mod.write_hard_constraint_mask(
        out_gpkg=out_path,
        snapshot_date=snapshot_date,
        snapshots_dir=snapshots_dir,
        include_aquaculture=False,
    )

    assert written == out_path
    assert out_path.exists()

    out = gpd.read_file(out_path)
    assert len(out) > 0
    assert set(out.geom_type.unique().tolist()).issubset({"Polygon", "MultiPolygon"})


def test_latest_snapshot_date_uses_latest_directory(tmp_path: Path) -> None:
    snapshots = tmp_path / "snapshots"
    (snapshots / "2026-01-01").mkdir(parents=True)
    (snapshots / "2026-02-15").mkdir(parents=True)
    (snapshots / "2025-12-31").mkdir(parents=True)

    got = mod.latest_snapshot_date(snapshots)
    assert got == "2026-02-15"


def test_snapshot_geojson_for_source_raises_if_missing(
    snapshots_dir: Path,
    snapshot_date: str,
) -> None:
    src = RemoteSource(
        key="missing_source",
        name="Missing Source",
        kind="arcgis_featureserver",
        url="https://example.com/missing",
        purpose="Missing source test",
        expected_geometry="Polygon",
        expected_crs="EPSG:4326",
        tags=("test",),
    )

    with pytest.raises(FileNotFoundError):
        mod.snapshot_geojson_for_source(
            src,
            date_str=snapshot_date,
            snapshots_dir=snapshots_dir,
        )