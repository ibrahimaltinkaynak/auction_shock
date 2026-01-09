# Fallback Contract (v1)

Goal: Ensure ingestion continuity if the primary source fails, without changing product scope.

## Primary source (v1)
- FiscalData: Treasury Securities Auctions Data (API)

## Fallback source (v1 stub)
- TreasuryDirect: Auction Query export (CSV/JSON/XML) OR official endpoint used by Auction Query

## Contract: Fetcher interface
Input: FetchRequest
- start_date: YYYY-MM-DD
- end_date: YYYY-MM-DD
- security_type: optional
- tenor: optional

Output: FetchResponse
- source_name
- retrieved_at_utc (UTC ISO8601)
- request_url
- request_params
- content_type
- body_bytes (raw bytes, unchanged)

## Normalization requirements (v1)
- Raw capture must be stored unchanged (bytes).
- A SHA-256 hash must be computed on raw bytes.
- Normalization must map raw fields into our normalized schema without interpretation.

## Minimal acceptance test (smoke)
- Fetch returns HTTP 200 (or equivalent success) and non-empty body.
- No normalization required for smoke test.

