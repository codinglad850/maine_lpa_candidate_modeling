#!/usr/bin/env python3

from __future__ import annotations

import json
from datetime import date, datetime, UTC
from pathlib import Path

import geopandas as gpd
import requests

from src.paths import (
    snapshot_file_path,
    snapshot_manifest_path,
)
from src.sources.common import RemoteSource
from src.sources.candidate_generation import CANDIDATE_GENERATION_REMOTE_SOURCES
from src.sources.constraints import HARD_CONSTRAINT_REMOTE_SOURCES
from src.sources.access import ACCESS_REMOTE_SOURCES, TOWNS_LAYER
from src.sources.depth import COASTAL_MAINE_TOPOBATHY_URLLIST

HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
}

ARCGIS_BATCH = 1000
WGS84 = "EPSG:4326"


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def arcgis_query_url(src: RemoteSource) -> str:
    url = src.url.rstrip("/")
    return url if url.endswith("/query") else f"{url}/query"


def download_arcgis_geojson_features(src: RemoteSource) -> gpd.GeoDataFrame:
    layer_url = arcgis_query_url(src)
    features = []
    offset = 0

    while True:
        params = {
            "where": "1=1",
            "outFields": "*",
            "returnGeometry": "true",
            "f": "geojson",
            "resultOffset": offset,
            "resultRecordCount": ARCGIS_BATCH,
        }

        r = requests.get(
            layer_url,
            params=params,
            headers=HTTP_HEADERS,
            timeout=180,
        )
        r.raise_for_status()

        data = r.json()
        if "error" in data:
            raise RuntimeError(f"ArcGIS error for {src.key} at offset {offset}: {data['error']}")

        batch = data.get("features", [])
        if not batch:
            break

        features.extend(batch)
        offset += ARCGIS_BATCH

        if len(batch) < ARCGIS_BATCH:
            break

    if not features:
        raise RuntimeError(f"No features downloaded for {src.key}")

    gdf = gpd.GeoDataFrame.from_features(features)
    if gdf.crs is None:
        gdf = gdf.set_crs(WGS84)
    else:
        gdf = gdf.to_crs(WGS84)

    gdf["__src_key"] = src.key
    gdf["__src_name"] = src.name
    gdf["__src_url"] = src.url
    return gdf


def download_direct_geo_file(src: RemoteSource, date_str: str, group: str) -> gpd.GeoDataFrame:
    r = requests.get(src.url, headers=HTTP_HEADERS, timeout=180)
    r.raise_for_status()

    tmp_path = snapshot_file_path(date_str, group, f"__tmp_{src.key}", ".geojson")
    ensure_parent(tmp_path)
    tmp_path.write_bytes(r.content)

    try:
        gdf = gpd.read_file(tmp_path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()

    if gdf.crs is None:
        gdf = gdf.set_crs(WGS84)
    else:
        gdf = gdf.to_crs(WGS84)

    gdf["__src_key"] = src.key
    gdf["__src_name"] = src.name
    gdf["__src_url"] = src.url
    return gdf


def snapshot_remote_geodata(src: RemoteSource, date_str: str, group: str) -> dict:
    out_path = snapshot_file_path(date_str, group, src.key, ".geojson")
    ensure_parent(out_path)

    if src.kind in {"arcgis_featureserver", "arcgis_mapserver", "arcgis_query"}:
        gdf = download_arcgis_geojson_features(src)
    else:
        gdf = download_direct_geo_file(src, date_str, group)

    gdf.to_file(out_path, driver="GeoJSON")

    return {
        "key": src.key,
        "name": src.name,
        "kind": src.kind,
        "url": src.url,
        "output_path": str(out_path),
        "row_count": int(len(gdf)),
        "crs": str(gdf.crs),
        "status": "ok",
    }


def snapshot_text_source(src: RemoteSource, date_str: str, group: str) -> dict:
    out_path = snapshot_file_path(date_str, group, src.key, ".txt")
    ensure_parent(out_path)

    r = requests.get(src.url, headers=HTTP_HEADERS, timeout=180)
    r.raise_for_status()
    out_path.write_bytes(r.content)

    return {
        "key": src.key,
        "name": src.name,
        "kind": src.kind,
        "url": src.url,
        "output_path": str(out_path),
        "bytes": out_path.stat().st_size,
        "status": "ok",
    }


def main() -> None:
    date_str = date.today().isoformat()

    groups = [
        ("candidate_generation", CANDIDATE_GENERATION_REMOTE_SOURCES),
        ("constraints", HARD_CONSTRAINT_REMOTE_SOURCES),
        ("access", {**ACCESS_REMOTE_SOURCES, TOWNS_LAYER.key: TOWNS_LAYER}),
    ]

    manifest = {
        "created_at_utc": datetime.now(UTC).isoformat(),
        "snapshot_date": date_str,
        "groups": {},
        "notes": [
            "DEM TIFF rasters are intentionally not snapshotted.",
            "Remote vector/text sources are snapshotted into dated directories.",
        ],
    }

    for group_name, sources in groups:
        print(f"\nSnapshotting group: {group_name}")
        group_results = []

        for src in sources.values():
            if not src.snapshot_recommended:
                print(f"Skipping {src.key} (snapshot_recommended=False)")
                continue

            try:
                print(f"  -> {src.key}")
                result = snapshot_remote_geodata(src, date_str, group_name)
            except Exception as e:
                result = {
                    "key": src.key,
                    "name": src.name,
                    "kind": src.kind,
                    "url": src.url,
                    "status": "error",
                    "error": str(e),
                }

            group_results.append(result)

        manifest["groups"][group_name] = group_results

    try:
        print("\nSnapshotting depth URL list")
        depth_result = snapshot_text_source(
            COASTAL_MAINE_TOPOBATHY_URLLIST,
            date_str,
            "candidate_generation",
        )
    except Exception as e:
        depth_result = {
            "key": COASTAL_MAINE_TOPOBATHY_URLLIST.key,
            "name": COASTAL_MAINE_TOPOBATHY_URLLIST.name,
            "kind": COASTAL_MAINE_TOPOBATHY_URLLIST.kind,
            "url": COASTAL_MAINE_TOPOBATHY_URLLIST.url,
            "status": "error",
            "error": str(e),
        }

    manifest["groups"]["depth_manifest_only"] = [depth_result]

    manifest_path = snapshot_manifest_path(date_str)
    ensure_parent(manifest_path)
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    print(f"\nWrote snapshot manifest: {manifest_path}")


if __name__ == "__main__":
    main()