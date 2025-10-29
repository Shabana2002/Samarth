import os
from pathlib import Path
import pandas as pd

# API keys and resource IDs
DATA_GOV_API_KEY = os.environ.get("DATA_GOV_API_KEY")
CROP_RESOURCE_ID = os.environ.get("CROP_RESOURCE_ID")
RAINFALL_RESOURCE_ID = os.environ.get("RAINFALL_RESOURCE_ID")

# Local fallback CSV paths
LOCAL_CROP_CSV = "crop_data_long.csv"
LOCAL_RAINFALL_CSV = "rainfall_data.csv"

# Cache directory
CACHE_DIR = "cache"
Path(CACHE_DIR).mkdir(parents=True, exist_ok=True)

# Utility functions to load CSVs safely
def load_crop_data():
    if os.path.exists(LOCAL_CROP_CSV):
        return pd.read_csv(LOCAL_CROP_CSV)
    else:
        raise FileNotFoundError(f"{LOCAL_CROP_CSV} not found")

def load_rainfall_data():
    if os.path.exists(LOCAL_RAINFALL_CSV):
        return pd.read_csv(LOCAL_RAINFALL_CSV)
    else:
        raise FileNotFoundError(f"{LOCAL_RAINFALL_CSV} not found")
