from __future__ import annotations

from .common import RemoteSource, source_dict


COASTAL_MAINE_TOPOBATHY_URLLIST = RemoteSource(
    key="coastal_maine_topobathy_urllist",
    name="NOAA Coastal Maine Topobathy 2022 URL List",
    kind="text_file",
    url=(
        "https://noaa-nos-coastal-lidar-pds.s3.amazonaws.com/dem/"
        "NGS_CoastalMaine_Topobathy_2022_10422/urllist10422.txt"
    ),
    purpose=(
        "Tile listing used to access NOAA topobathy rasters for candidate "
        "NAVD88 depth extraction."
    ),
    owner="NOAA",
    expected_geometry="Tabular",
    expected_crs=None,
    notes=(
        "This is a text manifest of raster tile URLs, not a spatial layer."
    ),
    snapshot_recommended=True,
    tags=("depth", "noaa", "topobathy", "remote"),
)

DEPTH_REMOTE_SOURCES = source_dict(
    COASTAL_MAINE_TOPOBATHY_URLLIST,
)