import datetime as dt
import requests
from .fetcher_base import BaseFetcher, FetchRequest, FetchResponse

class FiscalDataFetcher(BaseFetcher):
  source_name = "FISCALDATA_AUCTIONS_QUERY"

  URL = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/auctions_query"

  def fetch(self, req: FetchRequest) -> FetchResponse:
    retrieved_at_utc = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    params = {"page[size]": 1000}
    r = requests.get(self.URL, params=params, timeout=30)
    r.raise_for_status()

    return FetchResponse(
      source_name=self.source_name,
      retrieved_at_utc=retrieved_at_utc,
      request_url=r.url,
      request_params=params,
      content_type=r.headers.get("Content-Type", "application/octet-stream"),
      body_bytes=r.content
    )
