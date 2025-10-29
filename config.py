import os
from pathlib import Path

# API keys and resource IDs from environment variables
DATA_GOV_API_KEY = os.environ.get("DATA_GOV_API_KEY")
CROP_RESOURCE_ID = os.environ.get("CROP_RESOURCE_ID")
RAINFALL_RESOURCE_ID = os.environ.get("RAINFALL_RESOURCE_ID")

# Local fallback CSV paths
LOCAL_CROP_CSV = "crop_data_long.csv"
LOCAL_RAINFALL_CSV = "rainfall_data.csv"

# Cache directory
CACHE_DIR = "cache"
Path(CACHE_DIR).mkdir(parents=True, exist_ok=True)
