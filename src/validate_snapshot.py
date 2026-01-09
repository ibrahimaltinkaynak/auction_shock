import json
from pathlib import Path
import pandas as pd

HISTORY_PATH = Path("dist/library/history_snapshot.parquet")
PROOF_PATH = Path("dist/proof_packs/proof_missing_tail_proxy_730d_v5.json")
RUN_META_PATH = Path("dist/raw/fiscaldata/20260109T002921Z_730d_v5/RUN_META.json")

def ok(msg): print("OK:", msg)
def fail(msg): raise SystemExit("FAIL: " + msg)

def main():
  if not HISTORY_PATH.exists(): fail(f"missing {HISTORY_PATH}")
  ok(f"found {HISTORY_PATH}")

  if not PROOF_PATH.exists(): fail(f"missing {PROOF_PATH}")
  ok(f"found {PROOF_PATH}")

  if not RUN_META_PATH.exists(): fail(f"missing {RUN_META_PATH}")
  ok(f"found {RUN_META_PATH}")

  run_meta = json.loads(RUN_META_PATH.read_text(encoding="utf-8"))
  obs_min = run_meta.get("observed_min_auction_date")
  obs_max = run_meta.get("observed_max_auction_date")
  total_rows = int(run_meta.get("total_rows", -1))

  if not obs_min or not obs_max:
    fail("RUN_META missing observed_min_auction_date / observed_max_auction_date")
  ok("RUN_META has observed min/max")

  df = pd.read_parquet(HISTORY_PATH)

  if int(len(df)) != total_rows:
    fail(f"rows mismatch: got {len(df)} expected {total_rows}")
  ok(f"rows == {total_rows}")

  min_date = str(df["auction_date"].min())
  max_date = str(df["auction_date"].max())

  if min_date != obs_min:
    fail(f"min_date mismatch: got {min_date} expected {obs_min}")
  ok(f"min_date == {obs_min}")

  if max_date != obs_max:
    fail(f"max_date mismatch: got {max_date} expected {obs_max}")
  ok(f"max_date == {obs_max}")

  if "tail_kind" not in df.columns:
    fail("missing column: tail_kind")
  ok("tail_kind column present")

  tail_kinds = set(df["tail_kind"].dropna().astype(str).unique().tolist())
  for need in ("yield", "discount_rate", "missing"):
    if need not in tail_kinds:
      fail(f"tail_kind missing expected value: {need}")
  ok("tail_kind contains yield/discount_rate/missing")

  missing = int((df["tail_kind"] == "missing").sum())
  proof = json.loads(PROOF_PATH.read_text(encoding="utf-8"))

  if int(proof.get("missing_count_parquet", -1)) != missing:
    fail("proof missing_count_parquet mismatch vs parquet missing")
  ok("proof missing_count_parquet matches parquet missing")

  if int(proof.get("missing_count_found_in_raw", -1)) != missing:
    fail("proof missing_count_found_in_raw mismatch vs parquet missing")
  ok("proof missing_count_found_in_raw matches parquet missing")

  ok("validate_snapshot PASSED")

if __name__ == "__main__":
  main()
