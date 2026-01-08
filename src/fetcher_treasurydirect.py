from .fetcher_base import BaseFetcher, FetchRequest, FetchResponse

class TreasuryDirectFetcher(BaseFetcher):
  source_name = "TREASURYDIRECT_AUCTION_QUERY"

  def fetch(self, req: FetchRequest) -> FetchResponse:
    raise NotImplementedError("Fallback fetcher is a stub in v1. Use smoke_sources.py to validate reachability only.")
