import json
import hashlib
import datetime as dt
from pathlib import Path
import sqlite3
import pandas as pd

DB_PATH = Path("dist/library/event_library.sqlite")
HISTORY_PATH = Path("dist/library/history_snapshot.parquet")

def sha256_str(s: str) -> str:
  return hashlib.sha256(s.encode("utf-8")).hexdigest()

def norm_float(x):
  if x is None or x == "":
    return None
  try:
    return float(x)
  except Exception:
    return None

def norm_int(x):
  if x is None or x == "":
    return None
  try:
    return int(x)
  except Exception:
    return None

def load_all_records(run_dir: Path):
  records = []
  for p in sorted(run_dir.glob("page_*.json")):
    data = json.loads(p.read_text(encoding="utf-8"))
    for rec in data.get("data", []):
      records.append(rec)
  return records

def normalize_records(records):
  rows = []
  for r in records:
    cusip = r.get("cusip")
    auction_date = r.get("auction_date")
    security_term = r.get("security_term")
    security_type = r.get("security_type")
    reopening = norm_int(r.get("reopening")) or 0

    bid_to_cover = norm_float(r.get("bid_to_cover_ratio"))
    high_yield = norm_float(r.get("high_yield"))
    avg_med_yield = norm_float(r.get("avg_med_yield"))
    high_discnt_rate = norm_float(r.get("high_discnt_rate"))
    avg_med_discnt_rate = norm_float(r.get("avg_med_discnt_rate"))
    high_investment_rate = norm_float(r.get("high_investment_rate"))
    avg_med_investment_rate = norm_float(r.get("avg_med_investment_rate"))

    indirect_acc = norm_float(r.get("indirect_bidder_accepted"))
    total_acc = norm_float(r.get("total_accepted"))

    pct_indirect = None
    if indirect_acc is not None and total_acc not in (None, 0):
      pct_indirect = (indirect_acc / total_acc) * 100.0

    tail_bps = None
    tail_method = "missing"
    tail_kind = "missing"

    # Tail proxy (FACTUAL ONLY) — multi-kind
    # Only compute when both numbers exist. Never infer.
    if high_yield is not None and avg_med_yield is not None:
      tail_bps = (high_yield - avg_med_yield) * 100.0
      tail_method = "proxy_yield_high_minus_avgmed"
      tail_kind = "yield"
    elif high_discnt_rate is not None and avg_med_discnt_rate is not None:
      tail_bps = (high_discnt_rate - avg_med_discnt_rate) * 100.0
      tail_method = "proxy_discnt_high_minus_avgmed"
      tail_kind = "discount_rate"
    elif high_investment_rate is not None and avg_med_investment_rate is not None:
      tail_bps = (high_investment_rate - avg_med_investment_rate) * 100.0
      tail_method = "proxy_investment_high_minus_avgmed"
      tail_kind = "investment_rate"

    rec_hash = sha256_str(json.dumps(r, sort_keys=True, separators=(",", ":")))

    auction_id = f"{auction_date}_{cusip}"

    dq = []
    if bid_to_cover is None: dq.append("MISSING_BTC")
    if tail_bps is None: dq.append("MISSING_TAIL_PROXY")
    if pct_indirect is None: dq.append("MISSING_PCT_INDIRECT")

    rows.append({
      "auction_id": auction_id,
      "auction_date": auction_date,
      "cusip": cusip,
      "tenor": security_term,
      "security_type": security_type,
      "reopening": reopening,
      "issue_date": r.get("issue_date"),
      "maturity_date": r.get("maturity_date"),
      "bid_to_cover": bid_to_cover,
      "high_yield": high_yield,
      "avg_med_yield": avg_med_yield,
      "tail_bps": tail_bps,
      "tail_method": tail_method,
      "tail_kind": tail_kind,
      "pct_indirect": pct_indirect,
      "raw_record_hash": rec_hash,
      "data_quality_flags": "|".join(dq) if dq else ""
    })
  return pd.DataFrame(rows)

def upsert_sqlite(df):
  with sqlite3.connect(DB_PATH) as con:
    cur = con.cursor()
    for _, row in df.iterrows():
      cur.execute("""
        INSERT OR IGNORE INTO auctions
        (auction_id, auction_date, cusip, tenor, security_type, reopening, raw_hash)
        VALUES (?, ?, ?, ?, ?, ?, ?)
      """, (
        row["auction_id"],
        row["auction_date"],
        row["cusip"],
        row["tenor"],
        row["security_type"],
        int(row["reopening"]),
        row["raw_record_hash"]
      ))
    con.commit()

def write_history(df):
  HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
  if HISTORY_PATH.exists():
    old = pd.read_parquet(HISTORY_PATH)
    merged = pd.concat([old, df], ignore_index=True)
    merged = merged.drop_duplicates(subset=["auction_id"], keep="last")
  else:
    merged = df.drop_duplicates(subset=["auction_id"], keep="last")

  merged.to_parquet(HISTORY_PATH, index=False)
  return len(df), len(merged)

def main(run_dir_str: str):
  run_dir = Path(run_dir_str)
  records = load_all_records(run_dir)
  df = normalize_records(records)
  upsert_sqlite(df)
  added, total = write_history(df)

  out = {
    "run_dir": str(run_dir),
    "normalized_rows": int(len(df)),
    "history_total_rows": int(total),
    "created_at_utc": dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
  }
  print(json.dumps(out, indent=2))

if __name__ == "__main__":
  import sys
  if len(sys.argv) != 2:
    print("Usage: python -m src.ingest_run <run_dir>")
    raise SystemExit(2)
  main(sys.argv[1])
