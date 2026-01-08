import sys
import requests

def ok(msg: str):
  print("OK:", msg)

def fail(msg: str):
  print("FAIL:", msg)
  sys.exit(1)

def main():
  # FiscalData auctions endpoint
  fiscal_url = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/auctions_query"
  try:
    r = requests.get(fiscal_url, params={"page[size]": 1}, timeout=30)
    if r.status_code != 200:
      fail(f"FiscalData HTTP {r.status_code}")
    if not r.content:
      fail("FiscalData empty body")
    ok("FiscalData reachable (non-empty response)")
  except Exception as e:
    fail(f"FiscalData error: {e}")

  # TreasuryDirect Auction Query reachability (UI page)
  td_url = "https://www.treasurydirect.gov/auctions/auction-query/"
  try:
    r2 = requests.get(td_url, timeout=30)
    if r2.status_code != 200:
      fail(f"TreasuryDirect HTTP {r2.status_code}")
    if not r2.content:
      fail("TreasuryDirect empty body")
    ok("TreasuryDirect page reachable")
  except Exception as e:
    fail(f"TreasuryDirect error: {e}")

  ok("Smoke tests passed")

if __name__ == "__main__":
  main()
