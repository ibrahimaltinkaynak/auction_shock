import json
import hashlib
import datetime as dt
from pathlib import Path
import requests
from .config import FISCALDATA_ENDPOINT, FIELDS, PAGE_SIZE

def sha256_bytes(b: bytes) -> str:
  return hashlib.sha256(b).hexdigest()

def fetch_range(start_date: str, end_date: str, run_dir: Path) -> dict:
  run_dir.mkdir(parents=True, exist_ok=True)

  retrieved_at_utc = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
  fields_param = ",".join(FIELDS)

  page_number = 1
  total_rows = 0
  page_hashes = []

  while True:
    params = {
      "format": "json",
      "fields": fields_param,
      "filter": f"auction_date:gte:{start_date},auction_date:lte:{end_date}",
      "sort": "auction_date,security_term",
      "page[number]": page_number,
      "page[size]": PAGE_SIZE
    }

    r = requests.get(FISCALDATA_ENDPOINT, params=params, timeout=60)
    r.raise_for_status()

    body = r.content
    h = sha256_bytes(body)
    page_hashes.append(h)

    page_path = run_dir / f"page_{page_number:04d}.json"
    meta_path = run_dir / f"page_{page_number:04d}.meta.json"

    page_path.write_bytes(body)
    meta_path.write_text(json.dumps({
      "source": "FISCALDATA_AUCTIONS_QUERY",
      "retrieved_at_utc": retrieved_at_utc,
      "request_url": r.url,
      "request_params": params,
      "sha256": h
    }, indent=2), encoding="utf-8")

    data = json.loads(body.decode("utf-8"))
    rows = data.get("data", [])
    total_rows += len(rows)

    # pagination: links.next absent => fin
    links = data.get("links", {}) or {}
    if not links.get("next"):
      break

    page_number += 1
  # observed min/max auction_date in retrieved raw pages (not the requested filter bounds)
  observed_dates = []
  for pth in sorted(run_dir.glob("page_*.json")):
    data = json.loads(pth.read_text(encoding="utf-8"))
    for row in data.get("data", []):
      d = row.get("auction_date")
      if d:
        observed_dates.append(d)
  observed_min = min(observed_dates) if observed_dates else None
  observed_max = max(observed_dates) if observed_dates else None


  run_meta = {
    "requested_start_date": start_date,
    "requested_end_date": end_date,
    "observed_min_auction_date": observed_min,
    "observed_max_auction_date": observed_max,
    "retrieved_at_utc": retrieved_at_utc,
    "pages": page_number,
    "total_rows": total_rows,
    "page_sha256": page_hashes
  }
  (run_dir / "RUN_META.json").write_text(json.dumps(run_meta, indent=2), encoding="utf-8")
  return run_meta
