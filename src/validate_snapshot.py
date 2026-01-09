import json
from pathlib import Path
import sys

import pandas as pd

HISTORY_PATH = Path("dist/library/history_snapshot.parquet")
PROOF_PATH = Path("dist/proof_packs/proof_missing_tail_proxy_730d_v4.json")

EXPECTED = {
  "rows": 880,
  "min_date": "2024-01-10",
  "max_date": "2026-01-09",
  "missing_tail": 24,
}

def fail(msg: str) -> None:
  print("FAIL:", msg)
  raise SystemExit(2)

def ok(msg: str) -> None:
  print("OK:", msg)

def main() -> None:
  if not HISTORY_PATH.exists():
    fail(f"missing file: {HISTORY_PATH}")
  ok(f"found {HISTORY_PATH}")

  if not PROOF_PATH.exists():
    fail(f"missing file: {PROOF_PATH}")
  ok(f"found {PROOF_PATH}")

  df = pd.read_parquet(HISTORY_PATH)

  if len(df) != EXPECTED["rows"]:
    fail(f"rows mismatch: got {len(df)} expected {EXPECTED['rows']}")
  ok(f"rows == {EXPECTED['rows']}")

  min_date = str(df["auction_date"].min())
  max_date = str(df["auction_date"].max())

  if min_date != EXPECTED["min_date"]:
    fail(f"min_date mismatch: got {min_date} expected {EXPECTED['min_date']}")
  ok(f"min_date == {EXPECTED['min_date']}")

  if max_date != EXPECTED["max_date"]:
    fail(f"max_date mismatch: got {max_date} expected {EXPECTED['max_date']}")
  ok(f"max_date == {EXPECTED['max_date']}")

  if "tail_kind" not in df.columns:
    fail("missing column: tail_kind")
  ok("tail_kind column present")

  tail_kinds = set(df["tail_kind"].dropna().astype(str).unique().tolist())
  for need in ("yield", "discount_rate", "missing"):
    if need not in tail_kinds:
      fail(f"tail_kind missing expected value: {need}")
  ok("tail_kind contains yield/discount_rate/missing")

  missing = int((df["tail_kind"] == "missing").sum())
  if missing != EXPECTED["missing_tail"]:
    fail(f"missing tail mismatch: got {missing} expected {EXPECTED['missing_tail']}")
  ok(f"missing tail == {EXPECTED['missing_tail']}")

  proof = json.loads(PROOF_PATH.read_text(encoding="utf-8"))
  if int(proof.get("missing_count_parquet", -1)) != EXPECTED["missing_tail"]:
    fail("proof missing_count_parquet mismatch")
  ok("proof missing_count_parquet == 24")

  if int(proof.get("missing_count_found_in_raw", -1)) != EXPECTED["missing_tail"]:
    fail("proof missing_count_found_in_raw mismatch")
  ok("proof missing_count_found_in_raw == 24")

  ok("validate_snapshot PASSED")

if __name__ == "__main__":
  main()
