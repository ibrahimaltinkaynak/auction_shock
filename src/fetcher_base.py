from dataclasses import dataclass
from typing import Dict, Any, Optional

@dataclass(frozen=True)
class FetchRequest:
  start_date: str  # YYYY-MM-DD
  end_date: str    # YYYY-MM-DD
  security_type: Optional[str] = None  # e.g., "Note", "Bond", "Bill", "TIPS"
  tenor: Optional[str] = None          # e.g., "10Y", "2Y"

@dataclass(frozen=True)
class FetchResponse:
  source_name: str
  retrieved_at_utc: str
  request_url: str
  request_params: Dict[str, Any]
  content_type: str
  body_bytes: bytes

class BaseFetcher:
  source_name: str = "BASE"

  def fetch(self, req: FetchRequest) -> FetchResponse:
    raise NotImplementedError
