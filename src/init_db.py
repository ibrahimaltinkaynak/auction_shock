import sqlite3
from pathlib import Path

DB_PATH = Path("dist/library/event_library.sqlite")

SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS auctions (
  auction_id TEXT PRIMARY KEY,
  auction_date TEXT NOT NULL,
  cusip TEXT NOT NULL,
  tenor TEXT NOT NULL,
  security_type TEXT NOT NULL,
  reopening INTEGER NOT NULL,
  raw_hash TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS features (
  auction_id TEXT NOT NULL,
  rule_id TEXT NOT NULL,
  z_btc REAL,
  z_tail REAL,
  z_indirect REAL,
  composite_score REAL,
  percentile REAL,
  PRIMARY KEY (auction_id, rule_id),
  FOREIGN KEY (auction_id) REFERENCES auctions(auction_id)
);

CREATE TABLE IF NOT EXISTS events (
  event_id TEXT PRIMARY KEY,
  auction_id TEXT NOT NULL,
  rule_id TEXT NOT NULL,
  rarity_class TEXT NOT NULL,
  created_at_utc TEXT NOT NULL,
  FOREIGN KEY (auction_id) REFERENCES auctions(auction_id)
);
"""

def main():
  DB_PATH.parent.mkdir(parents=True, exist_ok=True)
  with sqlite3.connect(DB_PATH) as con:
    con.executescript(SCHEMA)
  print(f"OK: initialized {DB_PATH}")

if __name__ == "__main__":
  main()
