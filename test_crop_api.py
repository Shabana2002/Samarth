import requests

api_key = "579b464db66ec23bdd000001b99df47d66174b7771f8db787f44753c"
crop_resource_id = "35be999b-0208-4354-b557-f6ca9a5355de"
crop_url = f"https://api.data.gov.in/resource/{crop_resource_id}?api-key={api_key}&format=csv&limit=10000&offset=0"

response = requests.get(crop_url)

print("Status code:", response.status_code)

if response.status_code == 200:
    print("CSV data preview:\n")
    print(response.text[:500])  # prints first 500 characters
else:
    print("Error fetching data:", response.text)
