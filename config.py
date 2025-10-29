import os
from pathlib import Path

# Base directory (directory where this config.py resides)
BASE_DIR = Path(__file__).parent

# API keys and resource IDs from environment variables
DATA_GOV_API_KEY = os.environ.get("DATA_GOV_API_KEY")
CROP_RESOURCE_ID = os.environ.get("CROP_RESOURCE_ID")
RAINFALL_RESOURCE_ID = os.environ.get("RAINFALL_RESOURCE_ID")

# Local fallback CSV paths (absolute paths)
LOCAL_CROP_CSV = BASE_DIR / "crop_data_long.csv"
LOCAL_RAINFALL_CSV = BASE_DIR / "rainfall_data.csv"

# Cache directory
CACHE_DIR = BASE_DIR / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
