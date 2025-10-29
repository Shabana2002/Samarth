import requests
import pandas as pd

# Your API keys and resource IDs
DATA_GOV_API_KEY = "579b464db66ec23bdd000001b99df47d66174b7771f8db787f44753c"
RAINFALL_RESOURCE_ID = "6c05cd1b-ed59-40c2-bc31-e314f39c6971"

# Build API URL
url = f"https://api.data.gov.in/resource/{RAINFALL_RESOURCE_ID}?api-key={DATA_GOV_API_KEY}&format=json&limit=10"

try:
    resp = requests.get(url)
    resp.raise_for_status()  # Raise error for HTTP issues

    data = resp.json()
    records = data.get("records", [])

    if records:
        df = pd.DataFrame(records)
        print("[INFO] Live API data loaded successfully!")
        print(df.head())
    else:
        print("[WARN] API returned no records.")

except requests.exceptions.RequestException as e:
    print(f"[ERROR] Failed to fetch live API data: {e}")
