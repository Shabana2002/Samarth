import os
from pathlib import Path
import pandas as pd

# -----------------------------
# API keys and resource IDs
# -----------------------------
DATA_GOV_API_KEY: str = os.environ.get(
    "DATA_GOV_API_KEY",
    "579b464db66ec23bdd000001b99df47d66174b7771f8db787f44753c"
)
CROP_RESOURCE_ID: str = os.environ.get(
    "CROP_RESOURCE_ID",
    "35be999b-0208-4354-b557-f6ca9a5355de"
)
RAINFALL_RESOURCE_ID: str = os.environ.get(
    "RAINFALL_RESOURCE_ID",
    "6c05cd1b-ed59-40c2-bc31-e314f39c6971"
)

# -----------------------------
# Local fallback CSV paths
# -----------------------------
LOCAL_CROP_CSV: Path = Path("crop_data_long.csv")
LOCAL_RAINFALL_CSV: Path = Path("rainfall_data.csv")

# -----------------------------
# Cache directory
# -----------------------------
CACHE_DIR: Path = Path("cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------
# Utility functions
# -----------------------------
def load_csv_safe(file_path: Path) -> pd.DataFrame:
    """
    Safely load a CSV file, raise clear error if missing.
    """
    if file_path.exists():
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise RuntimeError(f"Failed to read CSV {file_path}: {e}")
    else:
        raise FileNotFoundError(f"CSV file not found: {file_path}")

def load_crop_data() -> pd.DataFrame:
    """
    Load crop data CSV.
    """
    return load_csv_safe(LOCAL_CROP_CSV)

def load_rainfall_data() -> pd.DataFrame:
    """
    Load rainfall data CSV.
    """
    return load_csv_safe(LOCAL_RAINFALL_CSV)
