import requests, json

URL = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/auctions_query"

params = {
  "format": "json",
  "page[size]": 1,
  "sort": "-auction_date"
}

r = requests.get(URL, params=params, timeout=30)
r.raise_for_status()
data = r.json()

rows = data.get("data", [])
if not rows:
  raise SystemExit("No data returned")

keys = sorted(rows[0].keys())
print("SAMPLE_KEYS_COUNT:", len(keys))
print("SAMPLE_KEYS:")
for k in keys:
  print(k)
