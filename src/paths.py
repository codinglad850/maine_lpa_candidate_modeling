from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"

CANONICAL_DIR = DATA_DIR / "canonical"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
OUTPUT_DIR = DATA_DIR / "outputs"
METADATA_DIR = DATA_DIR / "metadata"

HARD_CONSTRAINT_MASK = CANONICAL_DIR / "combined_hard_constraint_mask_inc_aq.gpkg"

# first candidate batch, screened for hard-restrictions
CANDIDATES_SCREENED = CANONICAL_DIR / "candidates" / "candidates_screened.geojson"

# candidate depth stage
CANDIDATES_DEPTH_NN = (
    INTERMEDIATE_DIR / "candidates" / "candidates_depth_nn.geojson"
)

CANDIDATES_DEPTH_INTERPOLATED = (
    INTERMEDIATE_DIR / "candidates" / "candidates_depth_interpolated.geojson"
)

CANDIDATES_DEPTH_NAN = (
    INTERMEDIATE_DIR / "candidates" / "candidates_depth_nan.geojson"
)

CANDIDATES_DEPTH_NAVD88 = (
    INTERMEDIATE_DIR / "candidates" / "candidates_depth_navd88.csv"
)

CANDIDATES_DEPTH_MLLW = (
    INTERMEDIATE_DIR / "candidates" / "candidates_depth_mllw.csv"
)

CACHE_DIR = DATA_DIR / "cache"

DEM_CACHE_DIR = CACHE_DIR / "dem_tifs"

ACCESS_GIS_CACHE_DIR = CACHE_DIR / "access_gis"

# navd88 to mllw depth conversion
VDATUM_RUN_LOG = METADATA_DIR / "runs" / "vdatum_conversion_log.json"

VDATUM_INPUT_CSV = (
    INTERMEDIATE_DIR / "candidates" / "vdatum_input_candidates_navd88.csv"
)

VDATUM_RUN_DIR = (
    INTERMEDIATE_DIR / "candidates" / "vdatum_run_candidates_mllw"
)

# candidates filtered for intertidal
CANDIDATES_INTERTIDAL = CANONICAL_DIR / "candidates" / "candidates_intertidal.geojson"

# access-stage artifacts
ACCESS_DIR = INTERMEDIATE_DIR / "access"

NORMALIZED_ACCESS_POINTS = (
    ACCESS_DIR / "normalized_access_points.geojson"
)

CANDIDATE_ACCESS_SCORED = (
    ACCESS_DIR / "candidate_access_scored.geojson"
)

LPA_ACCESS_SCORED = (
    ACCESS_DIR / "lpa_access_scored.geojson"
)

CANDIDATE_ACCESS_SCORED_CSV = (
    ACCESS_DIR / "candidate_access_scored.csv"
)

LPA_ACCESS_SCORED_CSV = (
    ACCESS_DIR / "lpa_access_scored.csv"
)

FEATURES_DIR = INTERMEDIATE_DIR / "features"
MODELS_DIR = OUTPUT_DIR / "models"

EMPIRICAL_MODEL_TRAINING_MATRIX = FEATURES_DIR / "maine_empirical_model_training_matrix.parquet"
EMPIRICAL_MODEL_FEATURES = FEATURES_DIR / "maine_empirical_model_features.parquet"
EMPIRICAL_MODEL_PKL = MODELS_DIR / "maine_empirical_model.pkl"
EMPIRICAL_MODEL_METADATA = MODELS_DIR / "maine_empirical_model_metadata.json"

TOWN_PRIOR_CSV = FEATURES_DIR / "town_prior.csv"

# final scored results based on models
CANDIDATES_SCORED = CANONICAL_DIR / "candidates" / "candidates_scored.geojson"
CANDIDATES_SCORED_CSV = CANONICAL_DIR / "candidates" / "candidates_scored.csv"

SCHEMAS_DIR = PROJECT_ROOT / "schemas"

HARD_CONSTRAINT_MASK_SCHEMA = SCHEMAS_DIR / "hard_constraint_mask.yaml"

CANDIDATES_SCREENED_SCHEMA = SCHEMAS_DIR / "candidates_screened.yaml"

CANDIDATES_DEPTH_NAVD88_SCHEMA = SCHEMAS_DIR / "candidates_depth_navd88.yaml"


CANDIDATES_INTERTIDAL_SCHEMA = SCHEMAS_DIR / "candidates_intertidal.yaml"
CANDIDATES_DEPTH_MLLW_SCHEMA = SCHEMAS_DIR / "candidates_depth_mllw.yaml"

NORMALIZED_ACCESS_POINTS_SCHEMA = SCHEMAS_DIR / "normalized_access_points.yaml"
CANDIDATE_ACCESS_SCORED_SCHEMA = SCHEMAS_DIR / "candidate_access_scored.yaml"
LPA_ACCESS_SCORED_SCHEMA = SCHEMAS_DIR / "lpa_access_scored.yaml"

EMPIRICAL_MODEL_FEATURES_SCHEMA = SCHEMAS_DIR / "maine_empirical_model_features.yaml"
EMPIRICAL_MODEL_TRAINING_MATRIX_SCHEMA = SCHEMAS_DIR / "maine_empirical_model_training_matrix.yaml"
TOWN_PRIOR_SCHEMA = SCHEMAS_DIR / "town_prior.yaml"
CANDIDATE_SPOTS_SCORED_SCHEMA = SCHEMAS_DIR / "candidate_spots_scored.yaml"