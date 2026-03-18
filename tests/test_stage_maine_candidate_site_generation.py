# tests/test_stage_maine_candidate_site_generation.py

from pathlib import Path

import geopandas as gpd
import pytest
from shapely.geometry import Polygon

import scripts.maine_candidate_site_generation as stage


@pytest.fixture
def approved_fc():
    poly = Polygon(
        [
            (-69.0000, 44.0000),
            (-68.9940, 44.0000),
            (-68.9940, 44.0060),
            (-69.0000, 44.0060),
            (-69.0000, 44.0000),
        ]
    )
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"NSSP": "A"},
                "geometry": poly.__geo_interface__,
            }
        ],
    }


@pytest.fixture
def hard_mask_gdf():
    poly = Polygon(
        [
            (-68.9982, 44.0018),
            (-68.9960, 44.0018),
            (-68.9960, 44.0042),
            (-68.9982, 44.0042),
            (-68.9982, 44.0018),
        ]
    )
    return gpd.GeoDataFrame(
        {"mask_id": [1]},
        geometry=[poly],
        crs=stage.WGS84,
    )


def _read_output(path: Path) -> gpd.GeoDataFrame:
    assert path.exists(), f"Expected output not written: {path}"
    gdf = gpd.read_file(path)
    assert not gdf.empty, "Stage wrote an empty output"
    return gdf


def _patch_stage_inputs(monkeypatch, tmp_path, approved_fc, hard_mask_gdf):
    out_path = tmp_path / "candidates_screened.geojson"

    monkeypatch.setattr(stage, "CANDIDATES_SCREENED", out_path)
    monkeypatch.setattr(stage, "arcgis_query_geojson", lambda layer_url, where: approved_fc)

    real_read_file = gpd.read_file

    def fake_read_file(path, *args, **kwargs):
        if Path(path) == Path(stage.HARD_CONSTRAINT_MASK):
            return hard_mask_gdf.copy()
        return real_read_file(path, *args, **kwargs)

    monkeypatch.setattr(stage.gpd, "read_file", fake_read_file)
    return out_path


@pytest.mark.regression
def test_main_writes_candidate_sites(monkeypatch, tmp_path, approved_fc, hard_mask_gdf):
    out_path = _patch_stage_inputs(monkeypatch, tmp_path, approved_fc, hard_mask_gdf)

    stage.main()
    out = _read_output(out_path)

    assert "candidate_id" in out.columns
    assert "geometry" in out.columns
    assert out.crs is not None
    assert out.crs.to_string().upper() == stage.WGS84


@pytest.mark.regression
def test_output_is_points_with_sequential_candidate_ids(monkeypatch, tmp_path, approved_fc, hard_mask_gdf):
    out_path = _patch_stage_inputs(monkeypatch, tmp_path, approved_fc, hard_mask_gdf)

    stage.main()
    out = _read_output(out_path)

    assert out.geometry.notna().all()
    assert out.geom_type.isin(["Point"]).all()
    assert out.is_valid.all()

    assert out["candidate_id"].tolist() == list(range(1, len(out) + 1))


@pytest.mark.regression
def test_all_output_points_fall_within_approved_area(monkeypatch, tmp_path, approved_fc, hard_mask_gdf):
    out_path = _patch_stage_inputs(monkeypatch, tmp_path, approved_fc, hard_mask_gdf)

    stage.main()
    out = _read_output(out_path)

    approved = stage.geojson_to_gdf(approved_fc).to_crs(stage.METRIC_CRS)
    approved_union = approved.geometry.union_all()

    out_m = out.to_crs(stage.METRIC_CRS)
    assert out_m.within(approved_union).all()


@pytest.mark.regression
def test_output_points_exclude_hard_mask(monkeypatch, tmp_path, approved_fc, hard_mask_gdf):
    out_path = _patch_stage_inputs(monkeypatch, tmp_path, approved_fc, hard_mask_gdf)

    stage.main()
    out = _read_output(out_path)

    out_m = out.to_crs(stage.METRIC_CRS)
    mask_m = hard_mask_gdf.to_crs(stage.METRIC_CRS)
    mask_union = mask_m.geometry.union_all()

    assert (~out_m.within(mask_union)).all(), "Some candidate points were not excluded by the hard mask"


@pytest.mark.regression
def test_regression_expected_count_and_coordinates(monkeypatch, tmp_path, approved_fc, hard_mask_gdf):
    out_path = _patch_stage_inputs(monkeypatch, tmp_path, approved_fc, hard_mask_gdf)

    stage.main()
    out = _read_output(out_path).sort_values("candidate_id").reset_index(drop=True)

    observed = [
        (int(row.candidate_id), round(row.geometry.x, 6), round(row.geometry.y, 6))
        for row in out.itertuples()
    ]

    EXPECTED = [(1, -68.996882, 44.004502)]

    if EXPECTED is None:
        pytest.fail(f"Set EXPECTED to this known-good snapshot:\n{observed}")

    assert observed == EXPECTED