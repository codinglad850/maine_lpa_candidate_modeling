import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

from src.paths import CANDIDATES_DEPTH_MLLW, CANDIDATES_INTERTIDAL

# --- helpers ---
def pick_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

df = pd.read_csv(CANDIDATES_DEPTH_MLLW)

# 1) detect lon/lat columns
lon_col = pick_col(df, ["lon", "longitude", "x", "Long", "LONG", "Lon"])
lat_col = pick_col(df, ["lat", "latitude", "y", "Lat", "LAT"])

if lon_col is None or lat_col is None:
    raise ValueError(f"Could not find lon/lat columns. Found columns: {list(df.columns)[:50]} ...")

mllw_col = 'depth_mllw'

# 3) clean sentinels -> NaN
SENTINELS = {-99999, -99999.0, -9999, -9999.0}

df[mllw_col] = pd.to_numeric(df[mllw_col], errors="coerce")
df.loc[df[mllw_col].isin(SENTINELS), mllw_col] = np.nan
vdatum_ok = df[mllw_col].notna()

# exposed at low tide datum (MLLW)
is_intertidal = vdatum_ok & (df[mllw_col] >= 0)

inter = df.loc[is_intertidal].copy()

# 5) build GeoDataFrame + export
geom = [Point(xy) for xy in zip(inter[lon_col].astype(float), inter[lat_col].astype(float))]
gdf = gpd.GeoDataFrame(inter, geometry=geom, crs="EPSG:4326")

# optional: keep output lighter (edit list as you like)
keep_cols = []
for c in ["id", "candidate_id", lon_col, lat_col, mllw_col]:
    if c and c in gdf.columns and c not in keep_cols:
        keep_cols.append(c)

# if you want ALL columns, comment out the next two lines
if keep_cols:
    gdf = gdf[keep_cols + ["geometry"]]

gdf.to_file(CANDIDATES_INTERTIDAL, driver="GeoJSON")
print(f"Wrote {CANDIDATES_INTERTIDAL} with {len(gdf):,} intertidal points")