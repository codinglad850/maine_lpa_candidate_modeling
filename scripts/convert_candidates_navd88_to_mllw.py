from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pandas as pd

from src.config import (
    JAVA_BIN,
    VDATUM_JAR,
    VDATUM_REGION,
    VDATUM_INPUT_HDATUM,
    VDATUM_INPUT_VDATUM,
    VDATUM_OUTPUT_HDATUM,
    VDATUM_OUTPUT_VDATUM,
)
from src.paths import (
    CANDIDATES_DEPTH_NAVD88,
    CANDIDATES_DEPTH_MLLW,
    VDATUM_INPUT_CSV,
    VDATUM_RUN_DIR,
)


REQUIRED_INPUT_COLUMNS = ["candidate_id", "lon", "lat", "depth_navd88"]


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def validate_input_df(df: pd.DataFrame) -> None:
    missing = [c for c in REQUIRED_INPUT_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required input columns: {missing}")

    if df["candidate_id"].isna().any():
        raise ValueError("Null candidate_id values found")

    if df["candidate_id"].duplicated().any():
        dupes = df.loc[df["candidate_id"].duplicated(), "candidate_id"].tolist()[:10]
        raise ValueError(f"Duplicate candidate_id values found: {dupes}")

    if df["lon"].isna().any() or df["lat"].isna().any():
        raise ValueError("Null lon/lat values found")

    if ((df["lon"] < -180) | (df["lon"] > 180)).any():
        raise ValueError("Invalid lon values outside [-180, 180]")

    if ((df["lat"] < -90) | (df["lat"] > 90)).any():
        raise ValueError("Invalid lat values outside [-90, 90]")

    if df["depth_navd88"].isna().any():
        raise ValueError("Null depth_navd88 values found")


def write_vdatum_input(df: pd.DataFrame, out_path: Path) -> None:
    ensure_parent(out_path)
    df[["lon", "lat", "depth_navd88"]].to_csv(out_path, index=False)


def prepare_vdatum_run_dir(run_dir: Path) -> None:
    if run_dir.exists():
        if run_dir.is_dir():
            shutil.rmtree(run_dir)
        else:
            raise RuntimeError(f"Expected VDATUM_RUN_DIR to be a directory path, got file: {run_dir}")
    run_dir.mkdir(parents=True, exist_ok=True)


def build_vdatum_command(input_csv: Path, output_dir: Path) -> list[str]:
    file_arg = (
        f'-file:txt:comma,0,1,2,append,skip1:"{input_csv}":"{output_dir}"'
    )

    return [
        JAVA_BIN,
        "-jar",
        str(VDATUM_JAR),
        f"ihorz:{VDATUM_INPUT_HDATUM}",
        f"ivert:{VDATUM_INPUT_VDATUM}",
        f"ohorz:{VDATUM_OUTPUT_HDATUM}",
        f"overt:{VDATUM_OUTPUT_VDATUM}",
        file_arg,
        f"region:{VDATUM_REGION}",
    ]


def run_vdatum(input_csv: Path, run_dir: Path) -> None:
    if not VDATUM_JAR.exists():
        raise FileNotFoundError(f"VDatum jar not found: {VDATUM_JAR}")

    prepare_vdatum_run_dir(run_dir)

    cmd = build_vdatum_command(input_csv, run_dir)

    print("Running VDatum...")
    print(" ".join(cmd))

    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=False,
    )

    if proc.stdout:
        print(proc.stdout)

    if proc.returncode != 0:
        raise RuntimeError(
            f"VDatum failed with return code {proc.returncode}\n"
            f"STDERR:\n{proc.stderr}"
        )

    if not run_dir.exists() or not run_dir.is_dir():
        raise RuntimeError(f"VDatum did not produce expected output directory: {run_dir}")


def find_single_csv(run_dir: Path) -> Path:
    csvs = sorted(run_dir.rglob("*.csv"))
    if not csvs:
        raise FileNotFoundError(f"No CSV found in VDatum run dir: {run_dir}")
    if len(csvs) > 1:
        raise RuntimeError(f"Multiple CSVs found in VDatum run dir: {csvs}")
    return csvs[0]


def find_log_files(run_dir: Path) -> list[Path]:
    logs = sorted(run_dir.rglob("*.log"))
    logs += sorted(run_dir.rglob("*.txt"))
    return logs


def read_vdatum_output(output_csv: Path, expected_rows: int) -> pd.DataFrame:
    if not output_csv.exists() or not output_csv.is_file():
        raise FileNotFoundError(f"VDatum output CSV not found: {output_csv}")

    # vdatum output has 2 header rows to start: the first is the columns i provided
    # the second is some custom header row
    df = pd.read_csv(output_csv, skiprows=1)

    if len(df) != expected_rows:
        raise ValueError(
            f"VDatum output row count mismatch: expected {expected_rows}, found {len(df)}"
        )

    return df

def validate_output_df(df: pd.DataFrame) -> None:
    required = [
        "candidate_id",
        "lon",
        "lat",
        "depth_navd88",
        "depth_mllw",
        "vdatum_status",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required output columns: {missing}")

    if df["candidate_id"].isna().any():
        raise ValueError("Null candidate_id values in output")

    if df["candidate_id"].duplicated().any():
        raise ValueError("Duplicate candidate_id values in output")

    success_mask = df["vdatum_status"] == "success"
    if success_mask.any() and df.loc[success_mask, "depth_mllw"].isna().any():
        raise ValueError("Some successful rows have null depth_mllw")


def main() -> None:
    print(f"Reading input: {CANDIDATES_DEPTH_NAVD88}")
    df_in = pd.read_csv(CANDIDATES_DEPTH_NAVD88)
     # vdatum cannot handle nans, reset index so that mllw assignment is coherent
    df_in = df_in.dropna(subset=["depth_navd88"]).reset_index(drop=True)
    validate_input_df(df_in)

    write_vdatum_input(df_in, VDATUM_INPUT_CSV)
    run_vdatum(VDATUM_INPUT_CSV, VDATUM_RUN_DIR)

    vdatum_csv = find_single_csv(VDATUM_RUN_DIR)
    log_files = find_log_files(VDATUM_RUN_DIR)

    print(f"Using VDatum output CSV: {vdatum_csv}")
    if log_files:
        print("VDatum log files:")
        for p in log_files:
            print(f"  - {p}")

    df_vdatum = read_vdatum_output(vdatum_csv, expected_rows=len(df_in))

    depth_mllw = df_vdatum['dstHeightZ']

    df_out = df_in.copy()

    df_out["depth_mllw"] = depth_mllw
    df_out["vdatum_status"] = df_out["depth_mllw"].notna().map(
        {True: "success", False: "failed"}
    )
    df_out["vdatum_input_datum"] = "NAVD88"
    df_out["vdatum_output_datum"] = "MLLW"
    df_out["vdatum_region"] = VDATUM_REGION
    df_out["vdatum_run_dir"] = str(VDATUM_RUN_DIR)
    df_out["vdatum_output_csv"] = str(vdatum_csv)

    validate_output_df(df_out)

    ensure_parent(CANDIDATES_DEPTH_MLLW)
    df_out.to_csv(CANDIDATES_DEPTH_MLLW, index=False)

    print(f"Wrote normalized output: {CANDIDATES_DEPTH_MLLW}")
    print(f"Success rows: {(df_out['vdatum_status'] == 'success').sum()}")
    print(f"Failed rows: {(df_out['vdatum_status'] != 'success').sum()}")


if __name__ == "__main__":
    main()