from __future__ import annotations

from .common import RemoteSource, source_dict


# -------------------------------------------------------------------
# Candidate-generation sources
#
# These are authoritative external inputs used to build the earliest
# candidate-point artifacts in the Maine LPA pipeline.
# -------------------------------------------------------------------


NSSP_APPROVED_AREAS = RemoteSource(
    key="nssp_approved_areas",
    name="Maine DMR Public Health NSSP Classifications",
    kind="arcgis_featureserver",
    url=(
        "https://services1.arcgis.com/RbMX0mRVOFNTdLzd/arcgis/rest/services/"
        "MaineDMR_Public_Health_2025_NSSP_Classifications/FeatureServer/81"
    ),
    purpose=(
        "Approved shellfish areas used as the allowed-area input to "
        "candidate site generation."
    ),
    owner="Maine DMR / ArcGIS",
    expected_geometry="Polygon",
    expected_crs="EPSG:4326",
    notes=(
        "Used by maine_candidate_site_generation.py. This source should be "
        "snapshotted for reproducibility before core pipeline runs."
    ),
    snapshot_recommended=True,
    tags=("candidate_generation", "approved_areas", "public_health", "remote"),
)


CANDIDATE_GENERATION_REMOTE_SOURCES = source_dict(
    NSSP_APPROVED_AREAS,
)