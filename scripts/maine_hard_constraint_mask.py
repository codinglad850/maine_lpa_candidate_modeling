import pandas as pd
import geopandas as gpd
from pathlib import Path

from src.sources.constraints import (
    CONSTRAINT_POLYGON_SOURCES,
    NOAA_NAV_SOURCES,
    HARD_CONSTRAINT_LOCAL_SOURCES,
    AQ_SOURCES,
    LPA_LAYER,
    FULL_AQUACULTURE_LAYER,
)

from src.paths import (
    HARD_CONSTRAINT_MASK,
    SNAPSHOTS_DIR,
    snapshot_file_path,
)

# ============================================================
# CONFIG
# ============================================================

FEET_TO_METERS = 0.3048
NOAA_BUFFER_M = 1000.0 * FEET_TO_METERS  # 304.8 m

METRIC_CRS = "EPSG:26919"
WGS84 = "EPSG:4326"

INCLUDE_AQUACULTURE = True
SNAPSHOT_GROUP = "constraints"

if INCLUDE_AQUACULTURE:
    DEFAULT_OUT_MASK_GPKG = HARD_CONSTRAINT_MASK
else:
    DEFAULT_OUT_MASK_GPKG = Path("me_geojson/combined_hard_constraint_mask_ex_aq.gpkg")


# ============================================================
# SNAPSHOT HELPERS
# ============================================================

def latest_snapshot_date(snapshots_dir: Path = SNAPSHOTS_DIR) -> str:
    """
    Return the lexicographically latest dated snapshot directory under data/snapshots.
    Assumes snapshot dirs are named YYYY-MM-DD.
    """
    if not snapshots_dir.exists():
        raise FileNotFoundError(f"Snapshots directory not found: {snapshots_dir}")

    candidates = [p.name for p in snapshots_dir.iterdir() if p.is_dir()]
    if not candidates:
        raise FileNotFoundError(f"No snapshot directories found in: {snapshots_dir}")

    return sorted(candidates)[-1]


def snapshot_geojson_for_source(
    src,
    date_str: str | None = None,
    snapshots_dir: Path = SNAPSHOTS_DIR,
    group: str = SNAPSHOT_GROUP,
) -> Path:
    if date_str is None:
        date_str = latest_snapshot_date(snapshots_dir)

    # snapshot_file_path uses the global SNAPSHOTS_DIR in paths.py, so for testability
    # we recreate the path directly if snapshots_dir is overridden.
    if snapshots_dir == SNAPSHOTS_DIR:
        p = snapshot_file_path(date_str, group, src.key, ".geojson")
    else:
        p = snapshots_dir / date_str / group / f"{src.key}.geojson"

    if not p.exists():
        raise FileNotFoundError(
            f"Snapshot not found for source {src.key}: {p}\n"
            f"Run scripts.snapshot_remote_sources first."
        )
    return p


# ============================================================
# READERS / BUILD HELPERS
# ============================================================

def _read_local_mask(path: str | Path) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(path)
    if gdf.crs is None:
        gdf = gdf.set_crs(WGS84)
    else:
        gdf = gdf.to_crs(WGS84)
    return gdf


def _read_snapshot_layer_as_gdf(
    src,
    date_str: str | None = None,
    snapshots_dir: Path = SNAPSHOTS_DIR,
) -> gpd.GeoDataFrame:
    p = snapshot_geojson_for_source(
        src,
        date_str=date_str,
        snapshots_dir=snapshots_dir,
    )
    gdf = gpd.read_file(p)

    if gdf.crs is None:
        gdf = gdf.set_crs(WGS84)
    else:
        gdf = gdf.to_crs(WGS84)

    if not gdf.empty:
        gdf["__src_name"] = src.key
        gdf["__src_url"] = src.url
        gdf["__snapshot_path"] = str(p)

    return gdf


def _buffer_if_needed(gdf_metric: gpd.GeoDataFrame, buffer_m: float) -> gpd.GeoDataFrame:
    if buffer_m <= 0:
        return gdf_metric

    out = gdf_metric.copy()
    out["geometry"] = out.geometry.buffer(buffer_m)
    out = out[~out.geometry.is_empty & out.geometry.notna()].copy()
    return out


def _load_aquaculture_parts(
    snapshot_date: str,
    snapshots_dir: Path,
    include_aquaculture: bool,
) -> list[gpd.GeoDataFrame]:
    parts: list[gpd.GeoDataFrame] = []

    if not include_aquaculture:
        return parts

    for src in AQ_SOURCES:
        print(f"Loading snapshot for {src.key} ...")
        gdf = _read_snapshot_layer_as_gdf(
            src,
            date_str=snapshot_date,
            snapshots_dir=snapshots_dir,
        )
        if gdf.empty:
            print(f"  -> no features in snapshot: {src.key}")
            continue

        gdf_m = gdf.to_crs(METRIC_CRS)
        gdf_m = _buffer_if_needed(gdf_m, NOAA_BUFFER_M)

        gdf_buff = gdf_m.to_crs(WGS84)
        gdf_buff["__src_name"] = f"{src.key}_buffer1000ft"
        gdf_buff["__src_url"] = src.url

        if src is LPA_LAYER:
            if "Status" in gdf_buff.columns:
                gdf_buff = gdf_buff[gdf_buff["Status"] == "A"]
            print(f"obtained {len(gdf_buff.index)} active LPAs")

        elif src is FULL_AQUACULTURE_LAYER:
            if "STATUS" in gdf_buff.columns:
                gdf_buff = gdf_buff[
                    (gdf_buff["STATUS"] == "A") | (gdf_buff["STATUS"] == "P")
                ]
            print(f"obtained {len(gdf_buff.index)} full active aquaculture operations")

        else:
            raise RuntimeError(f"Unhandled aquaculture source: {src.key}")

        parts.append(gdf_buff)

    return parts


def _load_local_parts() -> list[gpd.GeoDataFrame]:
    parts: list[gpd.GeoDataFrame] = []

    for src in HARD_CONSTRAINT_LOCAL_SOURCES.values():
        p = src.path_obj()

        if not p.exists():
            raise FileNotFoundError(f"{src.key} not found: {p}")

        gdf = _read_local_mask(p)
        gdf["__src_name"] = src.key
        gdf["__src_url"] = f"file://{p.resolve()}"

        parts.append(gdf)

    return parts


def _load_constraint_polygon_parts(
    snapshot_date: str,
    snapshots_dir: Path,
) -> list[gpd.GeoDataFrame]:
    parts: list[gpd.GeoDataFrame] = []

    for src in CONSTRAINT_POLYGON_SOURCES:
        print(f"Loading snapshot for {src.key} ...")
        gdf = _read_snapshot_layer_as_gdf(
            src,
            date_str=snapshot_date,
            snapshots_dir=snapshots_dir,
        )
        if gdf.empty:
            print(f"  -> no features in snapshot: {src.key}")
            continue

        gdf["__src_name"] = src.key
        gdf["__src_url"] = src.url
        parts.append(gdf)

    return parts


def _load_noaa_nav_parts(
    snapshot_date: str,
    snapshots_dir: Path,
) -> list[gpd.GeoDataFrame]:
    parts: list[gpd.GeoDataFrame] = []

    for src in NOAA_NAV_SOURCES:
        print(f"Loading snapshot for {src.key} ...")
        gdf = _read_snapshot_layer_as_gdf(
            src,
            date_str=snapshot_date,
            snapshots_dir=snapshots_dir,
        )
        if gdf.empty:
            print(f"  -> no features in snapshot: {src.key}")
            continue

        gdf_m = gdf.to_crs(METRIC_CRS)
        gdf_m = _buffer_if_needed(gdf_m, NOAA_BUFFER_M)

        gdf_buff = gdf_m.to_crs(WGS84)
        gdf_buff["__src_name"] = f"{src.key}_buffer1000ft"
        gdf_buff["__src_url"] = src.url
        parts.append(gdf_buff)

    return parts


def _combine_parts(parts: list[gpd.GeoDataFrame]) -> gpd.GeoDataFrame:
    if not parts:
        raise RuntimeError("No mask parts found.")

    combined = pd.concat(parts, ignore_index=True)
    combined = gpd.GeoDataFrame(combined, geometry="geometry", crs=parts[0].crs)
    combined_m = combined.to_crs(METRIC_CRS)

    print("Generating combined geometry geodataframe...")

    all_geoms = [g for g in combined_m.geometry if g is not None and not g.is_empty]
    out = gpd.GeoDataFrame(
        geometry=all_geoms,
        crs=METRIC_CRS,
    ).to_crs(WGS84)

    return out


# ============================================================
# PUBLIC STAGE API
# ============================================================

def build_hard_constraint_mask(
    snapshot_date: str | None = None,
    snapshots_dir: Path = SNAPSHOTS_DIR,
    include_aquaculture: bool = INCLUDE_AQUACULTURE,
) -> gpd.GeoDataFrame:
    """
    Build the hard constraint mask GeoDataFrame from local inputs and snapshotted
    remote sources. Returns the final output GeoDataFrame in WGS84.

    This is the function tests should call.
    """
    if snapshot_date is None:
        snapshot_date = latest_snapshot_date(snapshots_dir)

    print(f"Using snapshot date: {snapshot_date}")

    parts: list[gpd.GeoDataFrame] = []
    parts.extend(_load_aquaculture_parts(snapshot_date, snapshots_dir, include_aquaculture))
    parts.extend(_load_local_parts())
    parts.extend(_load_constraint_polygon_parts(snapshot_date, snapshots_dir))
    parts.extend(_load_noaa_nav_parts(snapshot_date, snapshots_dir))

    return _combine_parts(parts)


def write_hard_constraint_mask(
    out_gpkg: Path | str = DEFAULT_OUT_MASK_GPKG,
    snapshot_date: str | None = None,
    snapshots_dir: Path = SNAPSHOTS_DIR,
    include_aquaculture: bool = INCLUDE_AQUACULTURE,
) -> Path:
    """
    Build and write the hard constraint mask to GPKG. Returns the output path.
    """
    out_gpkg = Path(out_gpkg)

    gdf = build_hard_constraint_mask(
        snapshot_date=snapshot_date,
        snapshots_dir=snapshots_dir,
        include_aquaculture=include_aquaculture,
    )

    out_gpkg.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(out_gpkg, layer="mask", driver="GPKG")

    print(f"✅ Wrote {out_gpkg}")
    return out_gpkg


def main() -> None:
    write_hard_constraint_mask()


if __name__ == "__main__":
    main()