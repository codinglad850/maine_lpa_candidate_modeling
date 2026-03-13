from __future__ import annotations

from .common import RemoteSource, LocalSource, source_dict


# -------------------------------------------------------------------
# Local constraint masks
# -------------------------------------------------------------------

DANGERS_MASK = LocalSource(
    key="dangers_mask",
    name="Dangers mask",
    kind="local_file",
    path="data/raw/local/dangers_mask.geojson",
    purpose="Local exclusion mask used in hard-constraint construction.",
    expected_geometry="Polygon",
    expected_crs="EPSG:4326",
    tags=("constraints", "local"),
)

STRUCTURES_MASK = LocalSource(
    key="structures_mask",
    name="Structures mask",
    kind="local_file",
    path="data/raw/local/structures_mask.geojson",
    purpose="Local exclusion mask used in hard-constraint construction.",
    expected_geometry="Polygon",
    expected_crs="EPSG:4326",
    tags=("constraints", "local"),
)

MOORINGS_MASK = LocalSource(
    key="moorings_mask",
    name="Moorings mask",
    kind="local_file",
    path="data/raw/local/moorings_mask.geojson",
    purpose="Local exclusion mask used in hard-constraint construction.",
    expected_geometry="Polygon",
    expected_crs="EPSG:4326",
    tags=("constraints", "local"),
)

DOCKING_MASK = LocalSource(
    key="docking_mask",
    name="Docking mask",
    kind="local_file",
    path="data/raw/local/docking_mask.geojson",
    purpose="Local exclusion mask used in hard-constraint construction.",
    expected_geometry="Polygon",
    expected_crs="EPSG:4326",
    tags=("constraints", "local"),
)

NAV_ROUTES_MASK = LocalSource(
    key="nav_routes_mask",
    name="Navigation routes mask",
    kind="local_file",
    path="data/raw/local/nav_routes_mask.geojson",
    purpose="Local exclusion mask used in hard-constraint construction.",
    expected_geometry="Polygon",
    expected_crs="EPSG:4326",
    tags=("constraints", "local"),
)

ANCHORAGE_MASK = LocalSource(
    key="anchorage_mask",
    name="Anchorage mask",
    kind="local_file",
    path="data/raw/local/anchorage_mask.geojson",
    purpose="Local exclusion mask used in hard-constraint construction.",
    expected_geometry="Polygon",
    expected_crs="EPSG:4326",
    tags=("constraints", "local"),
)

BEACH_BUFFER = LocalSource(
    key="me_beach_buffer",
    name="Beach buffer",
    kind="local_file",
    path="data/raw/local/me_beach_buffer.geojson",
    purpose="Beach buffer exclusion mask.",
    expected_geometry="Polygon",
    expected_crs="EPSG:4326",
    tags=("constraints", "local"),
)

PROHIBITED_BUFFER = LocalSource(
    key="me_prohibited_300ft_buffer",
    name="300 ft prohibited buffer",
    kind="local_file",
    path="data/raw/local/me_prohibited_300ft_buffer.geojson",
    purpose="Regulatory exclusion buffer.",
    expected_geometry="Polygon",
    expected_crs="EPSG:4326",
    tags=("constraints", "local"),
)


# -------------------------------------------------------------------
# Remote habitat / regulatory layers
# -------------------------------------------------------------------

EELGRASS = RemoteSource(
    key="eelgrass",
    name="Maine DMR Eelgrass",
    kind="arcgis_featureserver",
    url="https://services1.arcgis.com/RbMX0mRVOFNTdLzd/ArcGIS/rest/services/MaineDMR_Eelgrass/FeatureServer/2",
    purpose="Constraint layer used in hard-mask construction.",
    expected_geometry="Polygon",
    expected_crs="EPSG:4326",
    tags=("constraints", "remote", "habitat"),
)

BALD_EAGLE_NEST_BUFFERS = RemoteSource(
    key="bald_eagle_nest_buffers",
    name="Bald Eagle Nests Maine 2023",
    kind="arcgis_featureserver",
    url="https://services.arcgis.com/QVENGdaPbd4LUkLV/arcgis/rest/services/Bald_Eagle_Nests_Maine_2023/FeatureServer/3",
    purpose="Constraint layer used in hard-mask construction.",
    expected_geometry="Polygon",
    tags=("constraints", "remote", "wildlife"),
)

SPECIAL_CONCERN_FISHES = RemoteSource(
    key="special_concern_fishes",
    name="Special concern fishes",
    kind="arcgis_featureserver",
    url="https://services1.arcgis.com/RbMX0mRVOFNTdLzd/arcgis/rest/services/MDIFW_SpecialConcernFishes/FeatureServer/3",
    purpose="Constraint layer used in hard-mask construction.",
    expected_geometry="Polygon",
    tags=("constraints", "remote"),
)

ROSEATE_TERN_EH = RemoteSource(
    key="roseate_tern_eh",
    name="Roseate tern essential habitat",
    kind="arcgis_featureserver",
    url="https://services1.arcgis.com/RbMX0mRVOFNTdLzd/arcgis/rest/services/EHRTERN/FeatureServer/0",
    purpose="Constraint layer used in hard-mask construction.",
    expected_geometry="Polygon",
    tags=("constraints", "remote"),
)

ENDANGERED_THREATENED_FISH = RemoteSource(
    key="endangered_threatened_fish",
    name="Endangered threatened fish",
    kind="arcgis_featureserver",
    url="https://services1.arcgis.com/RbMX0mRVOFNTdLzd/arcgis/rest/services/ET_Fish/FeatureServer/0",
    purpose="Constraint layer used in hard-mask construction.",
    expected_geometry="Polygon",
    tags=("constraints", "remote"),
)

USFWS_CRITICAL_HABITAT = RemoteSource(
    key="usfws_critical_habitat_final",
    name="USFWS critical habitat",
    kind="arcgis_featureserver",
    url="https://services.arcgis.com/QVENGdaPbd4LUkLV/arcgis/rest/services/USFWS_Critical_Habitat/FeatureServer/0",
    purpose="Constraint layer used in hard-mask construction.",
    expected_geometry="Polygon",
    tags=("constraints", "remote"),
)

PIPING_PLOVER_LEAST_TERN = RemoteSource(
    key="piping_plover_least_tern_eh",
    name="Piping plover / least tern habitat",
    kind="arcgis_featureserver",
    url="https://services1.arcgis.com/RbMX0mRVOFNTdLzd/arcgis/rest/services/EHPLVTRN/FeatureServer/0",
    purpose="Constraint layer used in hard-mask construction.",
    expected_geometry="Polygon",
    tags=("constraints", "remote"),
)

SPECIAL_CONCERN_WILDLIFE = RemoteSource(
    key="special_concern_wildlife",
    name="Special concern wildlife habitat",
    kind="arcgis_featureserver",
    url="https://services1.arcgis.com/RbMX0mRVOFNTdLzd/arcgis/rest/services/Special_Concern_Wildlife_Habitat_-_(Summarized)_view/FeatureServer/0",
    purpose="Constraint layer used in hard-mask construction.",
    expected_geometry="Polygon",
    tags=("constraints", "remote"),
)


# -------------------------------------------------------------------
# NOAA navigation channels
# -------------------------------------------------------------------

NOAA_MAINTAINED_CHANNELS_0 = RemoteSource(
    key="noaa_maintained_channels_0",
    name="NOAA maintained channels 0",
    kind="arcgis_featureserver",
    url="https://encdirect.noaa.gov/arcgis/rest/services/NavigationChartData/MarineTransportation/FeatureServer/0",
    purpose="Navigation channels buffered for exclusion.",
    expected_geometry="LineString",
    tags=("constraints", "remote", "noaa"),
)

NOAA_MAINTAINED_CHANNELS_1 = RemoteSource(
    key="noaa_maintained_channels_1",
    name="NOAA maintained channels 1",
    kind="arcgis_featureserver",
    url="https://encdirect.noaa.gov/arcgis/rest/services/NavigationChartData/MarineTransportation/FeatureServer/1",
    purpose="Navigation channels buffered for exclusion.",
    expected_geometry="LineString",
    tags=("constraints", "remote", "noaa"),
)


# -------------------------------------------------------------------
# Aquaculture layers used in mask construction
# -------------------------------------------------------------------

LPA_LAYER = RemoteSource(
    key="lpas",
    name="Maine LPAs",
    kind="arcgis_mapserver",
    url="https://gis.maine.gov/mapservices/rest/services/dmr/DMR_Aquaculture/MapServer/1",
    purpose="Existing LPAs used as exclusion mask.",
    expected_geometry="Polygon",
    tags=("constraints", "aquaculture"),
)

FULL_AQUACULTURE_LAYER = RemoteSource(
    key="full_aquaculture",
    name="Full aquaculture leases",
    kind="arcgis_mapserver",
    url="https://gis.maine.gov/mapservices/rest/services/dmr/DMR_Aquaculture/MapServer/2",
    purpose="Existing aquaculture used as exclusion mask.",
    expected_geometry="Polygon",
    tags=("constraints", "aquaculture"),
)


# -------------------------------------------------------------------
# Groups
# -------------------------------------------------------------------

HARD_CONSTRAINT_LOCAL_SOURCES = source_dict(
    DANGERS_MASK,
    STRUCTURES_MASK,
    MOORINGS_MASK,
    DOCKING_MASK,
    NAV_ROUTES_MASK,
    ANCHORAGE_MASK,
    BEACH_BUFFER,
    PROHIBITED_BUFFER,
)

HARD_CONSTRAINT_REMOTE_SOURCES = source_dict(
    EELGRASS,
    BALD_EAGLE_NEST_BUFFERS,
    SPECIAL_CONCERN_FISHES,
    ROSEATE_TERN_EH,
    ENDANGERED_THREATENED_FISH,
    USFWS_CRITICAL_HABITAT,
    PIPING_PLOVER_LEAST_TERN,
    SPECIAL_CONCERN_WILDLIFE,
    NOAA_MAINTAINED_CHANNELS_0,
    NOAA_MAINTAINED_CHANNELS_1,
    LPA_LAYER,
    FULL_AQUACULTURE_LAYER,
)

CONSTRAINT_POLYGON_SOURCES = [
    EELGRASS,
    BALD_EAGLE_NEST_BUFFERS,
    SPECIAL_CONCERN_FISHES,
    ROSEATE_TERN_EH,
    ENDANGERED_THREATENED_FISH,
    USFWS_CRITICAL_HABITAT,
    PIPING_PLOVER_LEAST_TERN,
    SPECIAL_CONCERN_WILDLIFE,
]

NOAA_NAV_SOURCES = [NOAA_MAINTAINED_CHANNELS_0, NOAA_MAINTAINED_CHANNELS_1]

AQ_SOURCES = [LPA_LAYER, FULL_AQUACULTURE_LAYER]