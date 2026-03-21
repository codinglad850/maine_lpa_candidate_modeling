# tests/test_stage_combine_candidates_with_bathymetry.py

from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Point

import scripts.combine_candidates_with_bathymetry as stage


@pytest.fixture
def candidates_gdf():
    return gpd.GeoDataFrame(
        {
            "candidate_id": [1, 2, 3, 4],
        },
        geometry=[
            Point(-68.9990, 44.0010),
            Point(-68.9980, 44.0020),
            Point(-68.9970, 44.0030),
            Point(-68.9960, 44.0040),
        ],
        crs="EPSG:4326",
    )


def _patch_stage(monkeypatch, tmp_path, candidates_gdf):
    out_nn = tmp_path / "candidates_depth_nn.geojson"
    out_interp = tmp_path / "candidates_depth_interpolated.geojson"
    out_nan = tmp_path / "candidates_depth_nan.geojson"
    out_csv = tmp_path / "candidates_depth_navd88.csv"

    monkeypatch.setattr(stage, "CANDIDATES_DEPTH_NN", out_nn)
    monkeypatch.setattr(stage, "CANDIDATES_DEPTH_INTERPOLATED", out_interp)
    monkeypatch.setattr(stage, "CANDIDATES_DEPTH_NAN", out_nan)
    monkeypatch.setattr(stage, "CANDIDATES_DEPTH_NAVD88", out_csv)

    real_read_file = gpd.read_file

    def fake_read_file(path, *args, **kwargs):
        if Path(path) == Path(stage.CANDIDATES_SCREENED):
            return candidates_gdf.copy()
        return real_read_file(path, *args, **kwargs)

    monkeypatch.setattr(stage.gpd, "read_file", fake_read_file)

    # Skip remote urllist fetch
    monkeypatch.setattr(
        stage,
        "fetch_text",
        lambda url: "\n".join(
            [
                "https://example.com/tile_0_dem.json",
                "https://example.com/tile_1_dem.json",
            ]
        ),
    )

    # Skip full DEM index construction
    fake_tiles = [
        {"tif_url": "https://example.com/tile_0.tif", "geom": None},
        {"tif_url": "https://example.com/tile_1.tif", "geom": None},
    ]
    monkeypatch.setattr(stage, "build_tile_index", lambda item_urls, cache_dir: (fake_tiles, object()))

    # Deterministic candidate -> tile assignment
    tile_map = {
        (-68.9990, 44.0010): 0,
        (-68.9980, 44.0020): 0,
        (-68.9970, 44.0030): 1,
        (-68.9960, 44.0040): 1,
    }

    def fake_find_tile_idx_for_point(rtree, tiles, x, y):
        return tile_map[(round(x, 4), round(y, 4))]

    monkeypatch.setattr(stage, "find_tile_idx_for_point", fake_find_tile_idx_for_point)

    # Deterministic bathymetry samples by tile
    def fake_sample_tile_nn_and_interp(
        tif_url,
        coords_lonlat,
        max_search_distance_m=12.0,
        smoothing_iterations=0,
    ):
        rounded = [(round(x, 4), round(y, 4)) for x, y in coords_lonlat]

        if tif_url.endswith("tile_0.tif"):
            # point 1 => nn, point 2 => interp
            assert rounded == [(-68.9990, 44.0010), (-68.9980, 44.0020)]
            return (
                pd.Series([-1.5, float("nan")], dtype="float64").to_numpy(),
                pd.Series([-1.5, -2.5], dtype="float64").to_numpy(),
            )

        if tif_url.endswith("tile_1.tif"):
            # point 3 => nn, point 4 => nan
            assert rounded == [(-68.9970, 44.0030), (-68.9960, 44.0040)]
            return (
                pd.Series([-3.5, float("nan")], dtype="float64").to_numpy(),
                pd.Series([-3.5, float("nan")], dtype="float64").to_numpy(),
            )

        raise AssertionError(f"Unexpected tif_url: {tif_url}")

    monkeypatch.setattr(stage, "sample_tile_nn_and_interp", fake_sample_tile_nn_and_interp)

    return out_nn, out_interp, out_nan, out_csv


def _read_geo(path: Path) -> gpd.GeoDataFrame:
    assert path.exists(), f"Missing expected output: {path}"
    gdf = gpd.read_file(path)
    return gdf


def _read_csv(path: Path) -> pd.DataFrame:
    assert path.exists(), f"Missing expected output: {path}"
    return pd.read_csv(path, keep_default_na=False, na_values=[''])


@pytest.mark.regression
def test_combine_candidates_with_bathymetry_writes_outputs(monkeypatch, tmp_path, candidates_gdf):
    out_nn, out_interp, out_nan, out_csv = _patch_stage(monkeypatch, tmp_path, candidates_gdf)

    stage.main()

    assert out_nn.exists()
    assert out_interp.exists()
    assert out_nan.exists()
    assert out_csv.exists()


@pytest.mark.regression
def test_combine_candidates_with_bathymetry_classifies_methods_correctly(monkeypatch, tmp_path, candidates_gdf):
    out_nn, out_interp, out_nan, out_csv = _patch_stage(monkeypatch, tmp_path, candidates_gdf)

    stage.main()

    df = _read_csv(out_csv).sort_values("candidate_id").reset_index(drop=True)

    assert df["candidate_id"].tolist() == [1, 2, 3, 4]
    assert df["depth_method"].tolist() == ["nn", "interp", "nn", "nan"]
    assert df["depth_navd88"].tolist() == pytest.approx([-1.5, -2.5, -3.5, float("nan")], nan_ok=True)


@pytest.mark.regression
def test_combine_candidates_with_bathymetry_splits_outputs_correctly(monkeypatch, tmp_path, candidates_gdf):
    out_nn, out_interp, out_nan, out_csv = _patch_stage(monkeypatch, tmp_path, candidates_gdf)

    stage.main()

    gdf_nn = _read_geo(out_nn).sort_values("candidate_id").reset_index(drop=True)
    gdf_interp = _read_geo(out_interp).sort_values("candidate_id").reset_index(drop=True)
    gdf_nan = _read_geo(out_nan).sort_values("candidate_id").reset_index(drop=True)

    assert gdf_nn["candidate_id"].tolist() == [1, 3]
    assert gdf_interp["candidate_id"].tolist() == [2]
    assert gdf_nan["candidate_id"].tolist() == [4]

    assert gdf_nn["depth_method"].tolist() == ["nn", "nn"]
    assert gdf_interp["depth_method"].tolist() == ["interp"]
    assert gdf_nan["depth_method"].tolist() == ["nan"]


@pytest.mark.regression
def test_combine_candidates_with_bathymetry_preserves_coordinates(monkeypatch, tmp_path, candidates_gdf):
    out_nn, out_interp, out_nan, out_csv = _patch_stage(monkeypatch, tmp_path, candidates_gdf)

    stage.main()

    df = _read_csv(out_csv).sort_values("candidate_id").reset_index(drop=True)

    assert df["lon"].tolist() == pytest.approx([-68.9990, -68.9980, -68.9970, -68.9960])
    assert df["lat"].tolist() == pytest.approx([44.0010, 44.0020, 44.0030, 44.0040])


@pytest.mark.regression
def test_combine_candidates_with_bathymetry_regression_snapshot(monkeypatch, tmp_path, candidates_gdf):
    out_nn, out_interp, out_nan, out_csv = _patch_stage(monkeypatch, tmp_path, candidates_gdf)

    stage.main()

    df = _read_csv(out_csv).sort_values("candidate_id").reset_index(drop=True)

    observed = df[["candidate_id", "lon", "lat", "depth_navd88", "depth_method"]].to_dict(orient="records")

    expected = [
        {
            "candidate_id": 1,
            "lon": -68.999,
            "lat": 44.001,
            "depth_navd88": -1.5,
            "depth_method": "nn",
        },
        {
            "candidate_id": 2,
            "lon": -68.998,
            "lat": 44.002,
            "depth_navd88": -2.5,
            "depth_method": "interp",
        },
        {
            "candidate_id": 3,
            "lon": -68.997,
            "lat": 44.003,
            "depth_navd88": -3.5,
            "depth_method": "nn",
        },
        {
            "candidate_id": 4,
            "lon": -68.996,
            "lat": 44.004,
            "depth_navd88": float("nan"),
            "depth_method": "nan",
        },
    ]

    for obs, exp in zip(observed, expected):
        assert obs["candidate_id"] == exp["candidate_id"]
        assert obs["lon"] == pytest.approx(exp["lon"])
        assert obs["lat"] == pytest.approx(exp["lat"])
        assert obs["depth_method"] == exp["depth_method"]
        if pd.isna(exp["depth_navd88"]):
            assert pd.isna(obs["depth_navd88"])
        else:
            assert obs["depth_navd88"] == pytest.approx(exp["depth_navd88"])