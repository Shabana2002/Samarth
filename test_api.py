import requests
from config import DATA_GOV_API_KEY, RAINFALL_RESOURCE_ID, CROP_RESOURCE_ID

def test_api(resource_id):
    url = f"https://api.data.gov.in/resource/{resource_id}?api-key={DATA_GOV_API_KEY}&format=json&offset=0&limit=5"
    r = requests.get(url)
    r.raise_for_status()
    data = r.json()
    print(f"DEBUG: API response keys for {resource_id}: {list(data.keys())}")
    # print first item if 'records' or 'data' exists
    if 'records' in data:
        print("First record:", data['records'][0])
    elif 'data' in data:
        print("First record:", data['data'][0])
    else:
        print("Full data:", data)

# Test both resources
test_api(RAINFALL_RESOURCE_ID)
test_api(CROP_RESOURCE_ID)
