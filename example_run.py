# example_run.py
from enhanced_handler import compare_states_average_rainfall, compare_districts, policy_advice
import requests

# Resource IDs
RAINFALL_RESOURCE_ID = "6c05cd1b-ed59-40c2-bc31-e314f39c6971"
CROP_RESOURCE_ID = "35be999b-0208-4354-b557-f6ca9a5355de"
DATA_GOV_API_KEY = "579b464db66ec23bdd000001b99df47d66174b7771f8db787f44753c"


def fetch_data(resource_id, limit=10000, offset=0):
    """Fetch data from Data.gov.in using GET (JSON format)."""
    url = f"https://api.data.gov.in/resource/{resource_id}"
    params = {
        "api-key": DATA_GOV_API_KEY,
        "format": "json",
        "limit": limit,
        "offset": offset
    }

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json().get("records", [])
        print(f"[INFO] Fetched {len(data)} records from resource {resource_id}")
        return data
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Live fetch failed: {e}")
        return None


if __name__ == "__main__":
    # Example 1: Average rainfall comparison
    try:
        print("\n=== Comparing average rainfall between Karnataka and Maharashtra ===")
        rainfall_data = fetch_data(RAINFALL_RESOURCE_ID)
        if rainfall_data is None:
            print("[WARN] Falling back to local CSV for rainfall.")
        res = compare_states_average_rainfall('Karnataka', 'Maharashtra', years=None, data=rainfall_data)
        print(res)
    except Exception as e:
        print("[Rainfall Example Failed]:", e)

    # Example 2: District-wise crop comparison
    try:
        print("\n=== Comparing districts for Wheat between Karnataka and Maharashtra ===")
        crop_data = fetch_data(CROP_RESOURCE_ID)
        if crop_data is None:
            print("[WARN] Falling back to local CSV for crop data.")
        res2 = compare_districts('Wheat', 'Karnataka', 'Maharashtra', data=crop_data)
        print(res2)
    except Exception as e:
        print("[District Compare Example Failed]:", e)

    # Example 3: Policy advice
    try:
        print("\n=== Policy advice for Rice vs Maize in Karnataka ===")
        crop_data = crop_data if crop_data else fetch_data(CROP_RESOURCE_ID)
        pa = policy_advice('Rice', 'Maize', 'Karnataka', years=None, data=crop_data)
        print(pa)
    except Exception as e:
        print("[Policy Advice Example Failed]:", e)
