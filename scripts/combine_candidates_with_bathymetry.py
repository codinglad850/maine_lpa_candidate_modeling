"""
Explicit tile indexing using Shapely STRtree (R-tree-like), then:
- nearest neighbor sample
- fill nodata holes
- bilinear sample
- classify points: nn / interp / nan
- output three GeoJSONs (+ optional CSV)
"""

from __future__ import annotations

import os
import time
import datetime as dt
import re

from urllib.parse import urlparse

import numpy as np
import requests
import geopandas as gpd
from shapely.geometry import box, Point
from rtree import index as rtree_index
from pathlib import Path

import rasterio
from pyproj import Transformer
from rasterio.fill import fillnodata
from rasterio.enums import Resampling
from rasterio.vrt import WarpedVRT
from rasterio.io import MemoryFile

from src.paths import CANDIDATES_SCREENED

from src.sources.depth import COASTAL_MAINE_TOPOBATHY_URLLIST

from src.paths import (
    CANDIDATES_DEPTH_NN,
    CANDIDATES_DEPTH_INTERPOLATED,
    CANDIDATES_DEPTH_NAN,
    CANDIDATES_DEPTH_NAVD88,
)

from src.paths import DEM_CACHE_DIR

CACHE_DIR = DEM_CACHE_DIR

# ----------------------------
# CONFIG
# ----------------------------

ITEM_SUFFIX = "_dem.json"

APPLY_10422_FIX = True

MAX_SEARCH_DISTANCE_M = 12.0
SMOOTHING_ITERATIONS = 0

# ----------------------------
# HTTP helpers
# ----------------------------

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "tile-dem-sampler/1.0"})

def fetch_text(url: str, timeout=60) -> str:
    r = SESSION.get(url, timeout=timeout)
    r.raise_for_status()
    return r.text

def fetch_json(url: str, timeout=60) -> dict:
    r = SESSION.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()

def maybe_fix_10422_href(href: str) -> str:
    if not APPLY_10422_FIX:
        return href
    if "_10422" in href:
        return href
    return href.replace(
        "/dem/NGS_CoastalMaine_Topobathy_2022/",
        "/dem/NGS_CoastalMaine_Topobathy_2022_10422/",
    )

# ----------------------------
# Sampling (nearest + fill holes + bilinear)
# ----------------------------

_TRANSFORMER_CACHE: dict[str, Transformer] = {}


def _get_transformer_to_ds(ds_crs) -> Transformer:
    key = ds_crs.to_string()
    tr = _TRANSFORMER_CACHE.get(key)
    if tr is None:
        tr = Transformer.from_crs("EPSG:4326", ds_crs, always_xy=True)
        _TRANSFORMER_CACHE[key] = tr
    return tr


def sample_tile_nn_and_interp(
    tif_url: str,
    coords_lonlat: list[tuple[float, float]],
    max_search_distance_m: float = 12.0,
    smoothing_iterations: int = 0,
) -> tuple[np.ndarray, np.ndarray]:
    print(f'processing tif url: {tif_url} with {len(coords_lonlat)} coordinates')

    local_tif = download_with_cache(tif_url, cache_dir=CACHE_DIR)

    with rasterio.open(local_tif, mode='r') as ds:
        if ds.crs is None:
            raise RuntimeError(f"DEM tile has no CRS: {tif_url}")

        tr = _get_transformer_to_ds(ds.crs)
        lons, lats = zip(*coords_lonlat)
        xs, ys = tr.transform(lons, lats)
        coords_ds = list(zip(xs, ys))

        # Nearest-neighbor on original raster
        nn_vals = np.array([v[0] for v in ds.sample(coords_ds)], dtype="float64")
        if ds.nodata is not None:
            nn_vals = np.where(nn_vals == ds.nodata, np.nan, nn_vals)

        raw_vals = ds.read(1)
        nan_count = (raw_vals == ds.nodata).sum()
        nan_rate = nan_count / (raw_vals.shape[0]*1.0*raw_vals.shape[1])
        print(f"tile nan rate: {nan_rate}")
        # Fill small nodata holes then bilinear sample
        band = ds.read(1, masked=True)

        res_x = float(abs(ds.res[0]))  # meters/pixel
        px = int(round(max_search_distance_m / res_x))
        px = max(1, px)

        filled = fillnodata(
            band.filled(np.nan).astype("float32"),
            mask=~band.mask,
            max_search_distance=px,
            smoothing_iterations=smoothing_iterations,
        )

        profile = ds.profile.copy()
        profile.update(dtype="float32", nodata=np.nan)

        with MemoryFile() as mem:
            with mem.open(**profile) as tmp:
                tmp.write(filled, 1)
                with WarpedVRT(tmp, crs=tmp.crs, resampling=Resampling.bilinear) as vrt:
                    interp_vals = np.array([v[0] for v in vrt.sample(coords_ds)], dtype="float64")

        interp_vals = np.where(np.isfinite(interp_vals), interp_vals, np.nan)
        interp_nans = np.isnan(interp_vals).sum()
        interp_non_nans = len(coords_lonlat) - interp_nans

        print(f"interp_nans: {interp_nans}, interp_non_nans: {interp_non_nans}")

        return nn_vals, interp_vals

def build_tile_index(item_urls: list[str], cache_dir: str):
    """
    Build list of tiles + an R-tree index over each tile's lon/lat bbox.

    Returns:
      tiles: list of dicts with:
        - "bbox": [minx, miny, maxx, maxy]  (EPSG:4326)
        - "geom": shapely polygon bbox
        - "tif_url": GeoTIFF href
        - "item_url": item json url
      rtree: rtree.index.Index with entries keyed by tile list index
    """
    p = rtree_index.Property()
    p.interleaved = True
    idx = rtree_index.Index(properties=p)

    tiles = []
    for k, item_url in enumerate(item_urls):
        item = fetch_json(item_url)

        bbox = item.get("bbox")
        if not bbox or len(bbox) != 4:
            continue

        assets = item.get("assets", {})
        tif_href = None
        for _, v in assets.items():
            href = v.get("href")
            if href and (href.lower().endswith(".tif") or href.lower().endswith(".tiff")):
                tif_href = href
                break
        if not tif_href:
            continue

        tif_url = maybe_fix_10422_href(tif_href)

        minx, miny, maxx, maxy = map(float, bbox)
        geom = box(minx, miny, maxx, maxy)

        tile_i = len(tiles)

        # store intended local cache file path (no download here)
        tif_cache_path = str(Path(cache_dir) / safe_filename_from_url(tif_url))

        tiles.append({
            "bbox": [minx, miny, maxx, maxy],
            "geom": geom,
            "tif_url": tif_url,
            "tif_cache_path": tif_cache_path,
            "item_url": item_url,
        })
        idx.insert(tile_i, (minx, miny, maxx, maxy))

        if (k + 1) % 200 == 0:
            print(f"Indexed {k+1}/{len(item_urls)} items... tiles={len(tiles)}")

    return tiles, idx

def safe_filename_from_url(url: str) -> str:
    """
    Turn a URL into a stable local filename.
    Keeps the basename but prefixes a short sanitized path hash-ish component
    to avoid collisions across folders with same basename.
    """
    u = urlparse(url)
    base = os.path.basename(u.path) or "tile.tif"
    # include a bit of the parent path to avoid collisions
    parent = os.path.dirname(u.path).strip("/").split("/")[-2:]
    prefix = "_".join(parent) if parent else "tile"
    prefix = re.sub(r"[^A-Za-z0-9_\-]+", "_", prefix)
    base = re.sub(r"[^A-Za-z0-9_\-\.]+", "_", base)
    return f"{prefix}__{base}"


def download_with_cache(url: str, cache_dir: str, timeout=120) -> str:
    """
    Download url to cache_dir if not already present.
    Uses a .part temp file and then atomic rename.
    Returns local filepath.
    """
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)

    fname = safe_filename_from_url(url)
    dst = cache_dir / fname
    if dst.exists() and dst.stat().st_size > 0:
        return str(dst)

    tmp = cache_dir / (fname + ".part")

    # Optional: try HEAD to get expected size (not always provided)
    expected = None
    try:
        h = SESSION.head(url, allow_redirects=True, timeout=timeout)
        if h.ok and h.headers.get("Content-Length"):
            expected = int(h.headers["Content-Length"])
    except Exception:
        pass

    # Stream download
    with SESSION.get(url, stream=True, timeout=timeout) as r:
        r.raise_for_status()
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024 * 8):  # 8MB
                if chunk:
                    f.write(chunk)

    # Basic integrity check
    if expected is not None:
        got = tmp.stat().st_size
        if got != expected:
            tmp.unlink(missing_ok=True)
            raise RuntimeError(f"Download size mismatch for {url}: expected={expected} got={got}")

    tmp.replace(dst)  # atomic rename on same filesystem
    return str(dst)

def find_tile_idx_for_point(rtree, tiles, x: float, y: float):
    """
    Return the best tile index containing point (x,y) in lon/lat (EPSG:4326), else None.
    If multiple tiles match (overlap), choose smallest-area bbox.
    """
    # Candidate tiles whose bbox intersects the point bbox
    hits = list(rtree.intersection((x, y, x, y)))
    if not hits:
        return None

    pt = Point(x, y)

    containing = []
    for t_i in hits:
        # Exact check using shapely bbox polygon
        if tiles[t_i]["geom"].covers(pt):  # covers includes boundary points
            containing.append(t_i)

    if not containing:
        return None

    if len(containing) == 1:
        return containing[0]

    # Choose smallest bbox area (most specific) if overlaps exist
    best = min(containing, key=lambda j: tiles[j]["geom"].area)
    return best

# ----------------------------
# Main
# ----------------------------

def main():
    if not os.path.exists(CANDIDATES_SCREENED):
        raise FileNotFoundError(f"Candidate GeoJSON not found: {CANDIDATES_SCREENED}")

    candidates = gpd.read_file(CANDIDATES_SCREENED)
    if candidates.crs is None:
        candidates = candidates.set_crs("EPSG:4326")
    candidates = candidates.to_crs("EPSG:4326").copy()

    candidates["lon"] = candidates.geometry.x.astype(float)
    candidates["lat"] = candidates.geometry.y.astype(float)

    n = len(candidates)
    print(f"Loaded candidates: {n}")
    print("Candidate bounds:", candidates.total_bounds)

    txt = fetch_text(COASTAL_MAINE_TOPOBATHY_URLLIST.url).splitlines()
    item_urls = [ln.strip() for ln in txt if ln.strip().lower().endswith(ITEM_SUFFIX)]
    if not item_urls:
        raise RuntimeError("No *_dem.json URLs found in urllist10422.txt")

    print(f'building tile index {dt.datetime.now()}')

    tiles, rtree = build_tile_index(item_urls, CACHE_DIR)
    if not tiles:
        raise RuntimeError("No DEM tiles indexed")

    print(f'built tile index {dt.datetime.now()}')

    # Assign each candidate to a tile
    # assumes candidates has columns lon/lat already (EPSG:4326)
    xs = candidates["lon"].to_numpy()
    ys = candidates["lat"].to_numpy()
    n = len(candidates)

    print(f'building candidate -> tile mapping {dt.datetime.now()}')

    tile_idx = np.full(n, -1, dtype=np.int32)
    for i in range(n):
        idx = find_tile_idx_for_point(rtree, tiles, float(xs[i]), float(ys[i]))
        tile_idx[i] = -1 if idx is None else idx

    print(f'built candidate -> tile mapping {dt.datetime.now()}')

    # Optional: build groups dict {tile_id: [row indices]}
    groups = {}
    for row_i, t_id in enumerate(tile_idx):
        if t_id == -1:
            continue
        groups.setdefault(int(t_id), []).append(row_i)

    print("Candidates with tile:", int(np.sum(tile_idx != -1)), "/", n)

    z_out = np.full(n, np.nan, dtype="float64")
    method = np.full(n, "nan", dtype=object)

    t0 = time.time()
    processed = 0

    for k, (t_id, rows) in enumerate(groups.items(), 1):
        tif_url = tiles[t_id]["tif_url"]
        coords = [(float(candidates.loc[r, "lon"]), float(candidates.loc[r, "lat"])) for r in rows]

        try:
            nn_vals, interp_vals = sample_tile_nn_and_interp(
                tif_url,
                coords,
                max_search_distance_m=MAX_SEARCH_DISTANCE_M,
                smoothing_iterations=SMOOTHING_ITERATIONS,
            )
        except Exception as e:
            print(f"[WARN] Tile {t_id} failed: {e}")
            processed += len(rows)
            continue

        for i, r in enumerate(rows):
            if not np.isnan(nn_vals[i]):
                z_out[r] = nn_vals[i]
                method[r] = "nn"
            elif not np.isnan(interp_vals[i]):
                z_out[r] = interp_vals[i]
                method[r] = "interp"

        processed += len(rows)

        if k % 25 == 0:
            elapsed = time.time() - t0
            print(f"Processed tiles: {k}/{len(groups)} | points: {processed}/{n} | elapsed: {elapsed:.1f}s")

    candidates["depth_navd88"] = z_out
    candidates["depth_method"] = method

    keep_cols = ["candidate_id", "lon", "lat", "depth_navd88", "depth_method", "geometry"]
    cand_nn = candidates[candidates["depth_method"] == "nn"][keep_cols].copy()
    cand_it = candidates[candidates["depth_method"] == "interp"][keep_cols].copy()
    cand_na = candidates[candidates["depth_method"] == "nan"][keep_cols].copy()

    cand_nn.to_file(CANDIDATES_DEPTH_NN, driver="GeoJSON")
    cand_it.to_file(CANDIDATES_DEPTH_INTERPOLATED, driver="GeoJSON")
    cand_na.to_file(CANDIDATES_DEPTH_NAN, driver="GeoJSON")

    out_csv = candidates[["candidate_id", "lon", "lat", "depth_navd88", "depth_method"]].copy()
    out_csv.to_csv(CANDIDATES_DEPTH_NAVD88, index=False)

    print("\nDone.")
    print(f"  NN:     {len(cand_nn):>8}")
    print(f"  Interp: {len(cand_it):>8}")
    print(f"  NaN:    {len(cand_na):>8}")
    print(f"  Overall NaN rate: {np.isnan(candidates['depth_navd88']).mean():.3f}")
    print("\nWrote:")
    print(" ", CANDIDATES_DEPTH_NN)
    print(" ", CANDIDATES_DEPTH_INTERPOLATED)
    print(" ", CANDIDATES_DEPTH_NAN)
    print(" ", CANDIDATES_DEPTH_NAVD88)


if __name__ == "__main__":
    main()