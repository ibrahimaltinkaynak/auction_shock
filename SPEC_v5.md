# SPEC v5 (CANONICAL)

## Purpose
Canonical audited snapshot built from FiscalData run *_730d_v5.

## Canonical inputs
- RUN_DIR: dist/raw/fiscaldata/*_730d_v5
- RUN_META.json must be inside RUN_DIR
- Proof pack: dist/proof_packs/proof_missing_tail_proxy_730d_v5.json

## Invariants (must never change)
- validate_snapshot requires explicit RUN_DIR
- validate_snapshot enforces RUN_DIR suffix == _730d_v5
- RUN_META must contain requested_* and observed_* keys
- Proof pack run_dir must match RUN_DIR
- Rebuild is reproducible via ./run_canon_v5.sh

## Rebuild
./run_canon_v5.sh
