from __future__ import annotations

from .common import LocalSource, RemoteSource, source_dict


# ------------------------------------------------------------
# Local access sources
# ------------------------------------------------------------

HARPSWELL_LAUNCHES_LANDINGS = LocalSource(
    key="harpswell_launches_landings",
    name="Harpswell launches and landings",
    kind="local_file",
    path="data/raw/local/harpswell_launches_landings.geojson",
    purpose="Access point source used in candidate and LPA access scoring.",
    expected_geometry="Point",
    expected_crs="EPSG:4326",
    tags=("access", "local"),
)

OSM_BOAT_ACCESS = LocalSource(
    key="osm_boat_access1",
    name="OSM boat access",
    kind="local_file",
    path="data/raw/local/osm_boat_access1.geojson",
    purpose="Access point source used in candidate and LPA access scoring.",
    expected_geometry="Point",
    expected_crs="EPSG:4326",
    tags=("access", "local"),
)


# ------------------------------------------------------------
# Remote access sources
# ------------------------------------------------------------

YARMOUTH_INTERTIDAL_ACCESS = RemoteSource(
    key="yarmouth_intertidal_access",
    name="Yarmouth Intertidal Access",
    kind="arcgis_featureserver",
    url="https://services8.arcgis.com/fNKHMoxC1Fcsh1mZ/arcgis/rest/services/Yarmouth_Intertidal_Access_PUBLIC_view/FeatureServer/0",
    purpose="Municipal access point source used in candidate and LPA access scoring.",
    expected_geometry="Point",
    expected_crs="EPSG:4326",
    tags=("access", "remote"),
)

PHIPPSBURG_INTERTIDAL_ACCESS = RemoteSource(
    key="phippsburg_intertidal_access",
    name="Phippsburg Intertidal Access",
    kind="arcgis_featureserver",
    url="https://services8.arcgis.com/fNKHMoxC1Fcsh1mZ/ArcGIS/rest/services/Phippsburg_intertidal_access_PUBLIC/FeatureServer/0",
    purpose="Municipal access point source used in candidate and LPA access scoring.",
    expected_geometry="Point",
    expected_crs="EPSG:4326",
    tags=("access", "remote"),
)

BRUNSWICK_INTERTIDAL_ACCESS = RemoteSource(
    key="brunswick_intertidal_access",
    name="Brunswick Intertidal Access",
    kind="arcgis_featureserver",
    url="https://services8.arcgis.com/fNKHMoxC1Fcsh1mZ/ArcGIS/rest/services/Brunswick_Intertidal_Access_2_view/FeatureServer/0",
    purpose="Municipal access point source used in candidate and LPA access scoring.",
    expected_geometry="Point",
    expected_crs="EPSG:4326",
    tags=("access", "remote"),
)

GEORGETOWN_INTERTIDAL_ACCESS = RemoteSource(
    key="georgetown_intertidal_access",
    name="Georgetown Intertidal Access",
    kind="arcgis_featureserver",
    url="https://services8.arcgis.com/fNKHMoxC1Fcsh1mZ/ArcGIS/rest/services/Georgetown_Intertidal_Access_PUBLIC_view_layer/FeatureServer/0",
    purpose="Municipal access point source used in candidate and LPA access scoring.",
    expected_geometry="Point",
    expected_crs="EPSG:4326",
    tags=("access", "remote"),
)

MAINE_GEOLIBRARY_STRUCTURE = RemoteSource(
    key="maine_geolibrary_structure",
    name="Maine GeoLibrary Structure",
    kind="arcgis_featureserver",
    url="https://services1.arcgis.com/RbMX0mRVOFNTdLzd/ArcGIS/rest/services/Maine_GeoLibrary_Structure/FeatureServer/1",
    purpose="Structure/access-related source used in candidate and LPA access scoring.",
    expected_geometry="Point",
    expected_crs="EPSG:4326",
    tags=("access", "remote"),
)

TOWNS_LAYER = RemoteSource(
    key="maine_towns",
    name="Maine Town and Townships Boundary Polygons",
    kind="arcgis_featureserver",
    url="https://services1.arcgis.com/RbMX0mRVOFNTdLzd/arcgis/rest/services/Maine_Town_and_Townships_Boundary_Polygons/FeatureServer/0",
    purpose="Optional town assignment layer used in access scoring outputs.",
    expected_geometry="Polygon",
    expected_crs="EPSG:4326",
    tags=("access", "towns", "optional"),
)


ACCESS_LOCAL_SOURCES = source_dict(
    HARPSWELL_LAUNCHES_LANDINGS,
    OSM_BOAT_ACCESS,
)

ACCESS_REMOTE_SOURCES = source_dict(
    YARMOUTH_INTERTIDAL_ACCESS,
    PHIPPSBURG_INTERTIDAL_ACCESS,
    BRUNSWICK_INTERTIDAL_ACCESS,
    GEORGETOWN_INTERTIDAL_ACCESS,
    MAINE_GEOLIBRARY_STRUCTURE,
)

ACCESS_ALL_SOURCES = source_dict(
    HARPSWELL_LAUNCHES_LANDINGS,
    OSM_BOAT_ACCESS,
    YARMOUTH_INTERTIDAL_ACCESS,
    PHIPPSBURG_INTERTIDAL_ACCESS,
    BRUNSWICK_INTERTIDAL_ACCESS,
    GEORGETOWN_INTERTIDAL_ACCESS,
    MAINE_GEOLIBRARY_STRUCTURE,
)