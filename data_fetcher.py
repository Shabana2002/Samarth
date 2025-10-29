import os
import requests
import pandas as pd
from pathlib import Path
from urllib.parse import urlencode
from config import DATA_GOV_API_KEY, CACHE_DIR

# Ensure cache directory exists
Path(CACHE_DIR).mkdir(parents=True, exist_ok=True)

def _resource_url(resource_id, limit=10000, offset=0, format='csv'):
    """
    Build the correct data.gov.in API URL.
    Format can be 'csv' or 'json'.
    """
    base = f"https://api.data.gov.in/resource/{resource_id}"
    params = {
        'api-key': DATA_GOV_API_KEY,
        'format': format,
        'limit': limit,
        'offset': offset
    }
    return f"{base}?{urlencode(params)}"


def fetch_resource_csv(resource_id, cache_name=None, use_cache=True, force_refresh=False):
    """
    Fetch resource data (CSV/JSON) from data.gov.in and cache locally.
    Falls back to cache if live fetch fails.
    """
    if cache_name is None:
        cache_name = f"{resource_id}.csv"
    cache_path = os.path.join(CACHE_DIR, cache_name)

    # Return cached version if available and not refreshing
    if use_cache and not force_refresh and os.path.exists(cache_path):
        try:
            print(f"[INFO] Using cached data from {cache_path}")
            return pd.read_csv(cache_path)
        except Exception:
            print("[WARN] Cached file corrupted — refetching.")
            os.remove(cache_path)

    # Build proper API URL
    url = _resource_url(resource_id, format='csv')
    print(f"[INFO] Fetching live data from: {url}")

    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"[ERROR] Live fetch failed: {e}")
        if os.path.exists(cache_path):
            print(f"[INFO] Falling back to cached {cache_path}")
            return pd.read_csv(cache_path)
        else:
            raise RuntimeError(f"Cannot fetch resource {resource_id} and no cache found.")

    # Save fetched CSV to cache
    with open(cache_path, 'wb') as f:
        f.write(resp.content)

    try:
        df = pd.read_csv(cache_path)
        print(f"[INFO] Cached new data ({len(df)} rows) → {cache_path}")
        return df
    except Exception as e:
        raise RuntimeError(f"Failed to read fetched CSV for {resource_id}: {e}")
