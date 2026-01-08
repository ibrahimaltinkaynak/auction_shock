FISCALDATA_ENDPOINT = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/auctions_query"

# Champs minimaux v1 (pas d'interprétation)
FIELDS = [
  "cusip",
  "security_type",
  "security_term",
  "auction_date",
  "issue_date",
  "maturity_date",
  "reopening",
  "bid_to_cover_ratio",
  "high_yield",
  "high_discnt_rate",
  "avg_med_discnt_rate",
  "high_investment_rate",
  "avg_med_investment_rate",
  "avg_med_yield",
  "indirect_bidder_accepted",
  "total_accepted"
]

PAGE_SIZE = 1000
