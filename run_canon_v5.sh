#!/usr/bin/env bash
set -euo pipefail

RUN_DIR="dist/raw/fiscaldata/20260109T002921Z_730d_v5"
PROOF_PATH="dist/proof_packs/proof_missing_tail_proxy_730d_v5.json"

echo "[1/5] wipe dist/library"
mkdir -p dist/library dist/proof_packs legacy/tmp_reset
cp -a dist/library "legacy/tmp_reset/library_$(date -u +"%Y%m%dT%H%M%SZ")" 2>/dev/null || true
rm -f dist/library/event_library.sqlite dist/library/event_library.sqlite-wal dist/library/event_library.sqlite-shm dist/library/history_snapshot.parquet

echo "[2/5] init_db"
python -m src.init_db

echo "[3/5] ingest_run"
python -m src.ingest_run "$RUN_DIR"

echo "[4/5] rebuild proof pack (v5)"
python - <<'PY'
import json, pandas as pd
from pathlib import Path

RUN_DIR = Path("dist/raw/fiscaldata/20260109T002921Z_730d_v5")
PROOF_DIR = Path("dist/proof_packs")
PROOF_DIR.mkdir(parents=True, exist_ok=True)

df = pd.read_parquet("dist/library/history_snapshot.parquet")
missing = df[df["tail_kind"] == "missing"][["auction_date","cusip","tenor","security_type"]].copy()
targets = {(r.auction_date, r.cusip) for r in missing.itertuples(index=False)}

found = {}
for page in sorted(RUN_DIR.glob("page_*.json")):
  data = json.loads(page.read_text(encoding="utf-8"))
  for row in data.get("data", []):
    k = (row.get("auction_date"), row.get("cusip"))
    if k in targets and k not in found:
      keep = [
        "auction_date","cusip","security_term","security_type",
        "high_yield","avg_med_yield",
        "high_discnt_rate","avg_med_discnt_rate",
        "high_investment_rate","avg_med_investment_rate",
        "reopening"
      ]
      found[k] = {"page": page.name, "raw": {x: row.get(x) for x in keep}}

proof = {
  "run_dir": str(RUN_DIR),
  "missing_count_parquet": int(len(missing)),
  "missing_count_found_in_raw": int(len(found)),
  "missing_targets": [{"auction_date": a, "cusip": c} for (a,c) in sorted(targets)],
  "raw_evidence": [{"auction_date": a, "cusip": c, **found[(a,c)]} for (a,c) in sorted(found)],
}

out_path = PROOF_DIR / "proof_missing_tail_proxy_730d_v5.json"
out_path.write_text(json.dumps(proof, indent=2), encoding="utf-8")
print("WROTE:", out_path)
print("missing_count_parquet:", proof["missing_count_parquet"])
print("missing_count_found_in_raw:", proof["missing_count_found_in_raw"])
PY

echo "[5/5] validate_snapshot (STRICT)"
python src/validate_snapshot.py "$RUN_DIR"

echo "OK: CANON v5 rebuilt + proof rebuilt + validated"
