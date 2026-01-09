import json
import sys
from pathlib import Path
import pandas as pd

HISTORY_PATH = Path("dist/library/history_snapshot.parquet")
PROOF_PATH = Path("dist/proof_packs/proof_missing_tail_proxy_730d_v5.json")
def ok(msg): print("OK:", msg)
def fail(msg): raise SystemExit("FAIL: " + msg)

def main(run_dir_arg: str) -> None:
  # strict mode: require explicit RUN_DIR
  if not run_dir_arg:
    print('FAIL: missing RUN_DIR argument')
    print('USAGE: python src/validate_snapshot.py dist/raw/fiscaldata/<RUN_ID>')
    raise SystemExit(2)

  run_dir = Path(run_dir_arg)
  # RUN_META must be inside the RUN_DIR we validate (no hardcode)
  run_meta_path = run_dir / "RUN_META.json"
  if not run_dir.exists():
    print(f'FAIL: RUN_DIR not found: {run_dir}')
    raise SystemExit(2)

  if not HISTORY_PATH.exists(): fail(f"missing {HISTORY_PATH}")
  ok(f"found {HISTORY_PATH}")

  if not PROOF_PATH.exists(): fail(f"missing {PROOF_PATH}")
  ok(f"found {PROOF_PATH}")

  if not run_meta_path.exists(): fail(f"missing {run_meta_path}")
  ok(f"found {run_meta_path}")

  run_meta = json.loads(run_meta_path.read_text(encoding="utf-8"))

  # strict: RUN_DIR must be the canonical v5 run
  run_name = run_dir.name
  if not run_name.endswith("_730d_v5"):
    fail(f"RUN_DIR must end with _730d_v5, got: {run_name}")
  ok("RUN_DIR suffix == _730d_v5")

  # strict: RUN_META required keys
  for k in ("requested_start_date","requested_end_date","observed_min_auction_date","observed_max_auction_date"):
    if k not in run_meta:
      fail(f"RUN_META missing key: {k}")
  ok("RUN_META has requested_* and observed_*")
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


  # strict: proof pack must refer to the same RUN_DIR we're validating
  proof_run_dir = str(proof.get("run_dir", ""))
  if proof_run_dir != str(run_dir):
    fail(f"proof run_dir mismatch: proof={proof_run_dir} validator={run_dir}")
  ok("proof run_dir matches validator RUN_DIR")
  if int(proof.get("missing_count_parquet", -1)) != missing:
    fail("proof missing_count_parquet mismatch vs parquet missing")
  ok("proof missing_count_parquet matches parquet missing")

  if int(proof.get("missing_count_found_in_raw", -1)) != missing:
    fail("proof missing_count_found_in_raw mismatch vs parquet missing")
  ok("proof missing_count_found_in_raw matches parquet missing")

  ok("validate_snapshot PASSED")

if __name__ == "__main__":
  main(sys.argv[1] if len(sys.argv) > 1 else "")
