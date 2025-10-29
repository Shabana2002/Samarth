import requests
import time

API_KEY = "579b464db66ec23bdd000001b99df47d66174b77753c"
RESOURCES = {
    "Rainfall Data": "6c05cd1b-ed59-40c2-bc31-e314f39c6971",
    "Crop Data": "35be999b-0208-4354-b557-f6ca9a5355de"
}
BASE_URL = "https://api.data.gov.in/resource/"


def fetch_all_records(resource_name, resource_id, chunk_size=1000, timeout=60):
    offset = 0
    all_records = []
    print(f"\nFetching {resource_name} ({resource_id}) in chunks of {chunk_size}...")

    # Detect the working API parameter
    api_param = "api-key"

    while True:
        params = {
            api_param: API_KEY,
            "format": "json",
            "limit": chunk_size,
            "offset": offset
        }
        try:
            response = requests.get(BASE_URL + resource_id, params=params, timeout=timeout)
            response.raise_for_status()
            data = response.json()
            records = data.get("records", [])

            if not records:
                break  # No more data

            all_records.extend(records)
            print(f"  Retrieved {len(records)} records, total so far: {len(all_records)}")
            offset += chunk_size
            time.sleep(0.2)  # polite pause between requests

        except requests.exceptions.RequestException as e:
            print(f"  Connection error at offset {offset}: {e}")
            break

    if not all_records:
        print("  No records retrieved.")
        return

    # Detect state column
    state_cols = [c for c in all_records[0].keys() if "state" in c.lower()]
    if not state_cols:
        print("  No state column detected.")
        return
    state_col = state_cols[0]

    # Extract distinct states
    states = sorted(set(r[state_col] for r in all_records if r.get(state_col)))
    print(f"  Number of distinct states: {len(states)}")
    print("  States:", states)


if __name__ == "__main__":
    print("=== Full API Fetch & State Listing for data.gov.in ===")
    for name, rid in RESOURCES.items():
        fetch_all_records(name, rid)
    print("\nFetch complete.")
