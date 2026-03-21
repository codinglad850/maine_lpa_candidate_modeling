# tests/test_stage_convert_candidates_navd88_to_mllw.py

from pathlib import Path

import pandas as pd
import pytest

import scripts.convert_candidates_navd88_to_mllw as stage


@pytest.fixture
def input_df():
    return pd.DataFrame(
        {
            "candidate_id": [1, 2, 3, 4],
            "lon": [-68.9990, -68.9980, -68.9970, -68.9960],
            "lat": [44.0010, 44.0020, 44.0030, 44.0040],
            "depth_navd88": [-1.5, -2.5, -3.5, -4.5],
            "depth_method": ["nn", "interp", "nn", "nn"],
        }
    )


@pytest.fixture
def vdatum_output_df():
    """
    Mimics the parsed VDatum output returned by read_vdatum_output(...).
    Only dstHeightZ is used by main().
    """
    return pd.DataFrame(
        {
            "dstHeightZ": [-1.1, None, -3.0, -4.2],
        }
    )


def _patch_stage(monkeypatch, tmp_path, input_df, vdatum_output_df):
    out_csv = tmp_path / "candidates_depth_mllw.csv"
    vdatum_input_csv = tmp_path / "vdatum_input.csv"
    vdatum_run_dir = tmp_path / "vdatum_run"
    fake_vdatum_output_csv = vdatum_run_dir / "result.csv"

    monkeypatch.setattr(stage, "CANDIDATES_DEPTH_MLLW", out_csv)
    monkeypatch.setattr(stage, "VDATUM_INPUT_CSV", vdatum_input_csv)
    monkeypatch.setattr(stage, "VDATUM_RUN_DIR", vdatum_run_dir)

    real_read_csv = pd.read_csv

    def fake_read_csv(path, *args, **kwargs):
        if Path(path) == Path(stage.CANDIDATES_DEPTH_NAVD88):
            return input_df.copy()
        return real_read_csv(path, *args, **kwargs)

    monkeypatch.setattr(stage.pd, "read_csv", fake_read_csv)

    def fake_run_vdatum(input_csv, run_dir):
        run_dir.mkdir(parents=True, exist_ok=True)
        fake_vdatum_output_csv.write_text("dummy\n", encoding="utf-8")

    monkeypatch.setattr(stage, "run_vdatum", fake_run_vdatum)
    monkeypatch.setattr(stage, "find_single_csv", lambda run_dir: fake_vdatum_output_csv)
    monkeypatch.setattr(stage, "find_log_files", lambda run_dir: [])
    monkeypatch.setattr(
        stage,
        "read_vdatum_output",
        lambda output_csv, expected_rows: vdatum_output_df.copy(),
    )

    return out_csv, vdatum_input_csv, vdatum_run_dir, fake_vdatum_output_csv


def _read_output(path: Path) -> pd.DataFrame:
    assert path.exists(), f"Expected output not written: {path}"
    return pd.read_csv(path)


@pytest.mark.regression
def test_convert_candidates_navd88_to_mllw_writes_output(monkeypatch, tmp_path, input_df, vdatum_output_df):
    out_csv, _, _, _ = _patch_stage(monkeypatch, tmp_path, input_df, vdatum_output_df)

    stage.main()

    out = _read_output(out_csv)
    assert len(out) == 4
    assert "depth_mllw" in out.columns
    assert "vdatum_status" in out.columns


@pytest.mark.regression
def test_convert_candidates_navd88_to_mllw_preserves_candidate_ids(monkeypatch, tmp_path, input_df, vdatum_output_df):
    out_csv, _, _, _ = _patch_stage(monkeypatch, tmp_path, input_df, vdatum_output_df)

    stage.main()

    out = _read_output(out_csv)
    assert out["candidate_id"].tolist() == [1, 2, 3, 4]


@pytest.mark.regression
def test_convert_candidates_navd88_to_mllw_assigns_expected_depths_and_status(monkeypatch, tmp_path, input_df, vdatum_output_df):
    out_csv, _, _, _ = _patch_stage(monkeypatch, tmp_path, input_df, vdatum_output_df)

    stage.main()

    out = _read_output(out_csv).sort_values("candidate_id").reset_index(drop=True)

    assert out["depth_mllw"].tolist() == pytest.approx([-1.1, float("nan"), -3.0, -4.2], nan_ok=True)
    assert out["vdatum_status"].tolist() == ["success", "failed", "success", "success"]


@pytest.mark.regression
def test_convert_candidates_navd88_to_mllw_adds_expected_metadata_columns(monkeypatch, tmp_path, input_df, vdatum_output_df):
    out_csv, _, vdatum_run_dir, fake_vdatum_output_csv = _patch_stage(monkeypatch, tmp_path, input_df, vdatum_output_df)

    stage.main()

    out = _read_output(out_csv)

    assert out["vdatum_input_datum"].tolist() == ["NAVD88"] * 4
    assert out["vdatum_output_datum"].tolist() == ["MLLW"] * 4
    assert out["vdatum_region"].tolist() == [int(stage.VDATUM_REGION)] * 4
    assert out["vdatum_run_dir"].tolist() == [str(vdatum_run_dir)] * 4
    assert out["vdatum_output_csv"].tolist() == [str(fake_vdatum_output_csv)] * 4


@pytest.mark.regression
def test_convert_candidates_navd88_to_mllw_writes_expected_vdatum_input(monkeypatch, tmp_path, input_df, vdatum_output_df):
    _, vdatum_input_csv, _, _ = _patch_stage(monkeypatch, tmp_path, input_df, vdatum_output_df)

    stage.main()

    assert vdatum_input_csv.exists(), "VDatum input CSV was not written"

    written = pd.read_csv(vdatum_input_csv)
    assert written.columns.tolist() == ["lon", "lat", "depth_navd88"]
    assert written["lon"].tolist() == pytest.approx([-68.9990, -68.9980, -68.9970, -68.9960])
    assert written["lat"].tolist() == pytest.approx([44.0010, 44.0020, 44.0030, 44.0040])
    assert written["depth_navd88"].tolist() == pytest.approx([-1.5, -2.5, -3.5, -4.5])


@pytest.mark.regression
def test_convert_candidates_navd88_to_mllw_regression_snapshot(monkeypatch, tmp_path, input_df, vdatum_output_df):
    out_csv, _, vdatum_run_dir, fake_vdatum_output_csv = _patch_stage(monkeypatch, tmp_path, input_df, vdatum_output_df)

    stage.main()

    out = _read_output(out_csv).sort_values("candidate_id").reset_index(drop=True)

    observed = out[
        [
            "candidate_id",
            "lon",
            "lat",
            "depth_navd88",
            "depth_mllw",
            "vdatum_status",
            "vdatum_input_datum",
            "vdatum_output_datum",
            "vdatum_region",
            "vdatum_run_dir",
            "vdatum_output_csv",
        ]
    ].to_dict(orient="records")

    expected = [
        {
            "candidate_id": 1,
            "lon": -68.999,
            "lat": 44.001,
            "depth_navd88": -1.5,
            "depth_mllw": -1.1,
            "vdatum_status": "success",
            "vdatum_input_datum": "NAVD88",
            "vdatum_output_datum": "MLLW",
            "vdatum_region": stage.VDATUM_REGION,
            "vdatum_run_dir": str(vdatum_run_dir),
            "vdatum_output_csv": str(fake_vdatum_output_csv),
        },
        {
            "candidate_id": 2,
            "lon": -68.998,
            "lat": 44.002,
            "depth_navd88": -2.5,
            "depth_mllw": float("nan"),
            "vdatum_status": "failed",
            "vdatum_input_datum": "NAVD88",
            "vdatum_output_datum": "MLLW",
            "vdatum_region": stage.VDATUM_REGION,
            "vdatum_run_dir": str(vdatum_run_dir),
            "vdatum_output_csv": str(fake_vdatum_output_csv),
        },
        {
            "candidate_id": 3,
            "lon": -68.997,
            "lat": 44.003,
            "depth_navd88": -3.5,
            "depth_mllw": -3.0,
            "vdatum_status": "success",
            "vdatum_input_datum": "NAVD88",
            "vdatum_output_datum": "MLLW",
            "vdatum_region": stage.VDATUM_REGION,
            "vdatum_run_dir": str(vdatum_run_dir),
            "vdatum_output_csv": str(fake_vdatum_output_csv),
        },
        {
            "candidate_id": 4,
            "lon": -68.996,
            "lat": 44.004,
            "depth_navd88": -4.5,
            "depth_mllw": -4.2,
            "vdatum_status": "success",
            "vdatum_input_datum": "NAVD88",
            "vdatum_output_datum": "MLLW",
            "vdatum_region": stage.VDATUM_REGION,
            "vdatum_run_dir": str(vdatum_run_dir),
            "vdatum_output_csv": str(fake_vdatum_output_csv),
        },
    ]

    for obs, exp in zip(observed, expected):
        assert obs["candidate_id"] == exp["candidate_id"]
        assert obs["lon"] == pytest.approx(exp["lon"])
        assert obs["lat"] == pytest.approx(exp["lat"])
        assert obs["depth_navd88"] == pytest.approx(exp["depth_navd88"])
        assert obs["vdatum_status"] == exp["vdatum_status"]
        assert obs["vdatum_input_datum"] == exp["vdatum_input_datum"]
        assert obs["vdatum_output_datum"] == exp["vdatum_output_datum"]
        assert obs["vdatum_region"] == int(exp["vdatum_region"])
        assert obs["vdatum_run_dir"] == exp["vdatum_run_dir"]
        assert obs["vdatum_output_csv"] == exp["vdatum_output_csv"]

        if pd.isna(exp["depth_mllw"]):
            assert pd.isna(obs["depth_mllw"])
        else:
            assert obs["depth_mllw"] == pytest.approx(exp["depth_mllw"])