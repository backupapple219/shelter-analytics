"""
01_ingest.py
------------
Pulls Austin Animal Center intake and outcome data from the
Socrata Open Data API (same API format used by Indianapolis IACS
and most municipal shelter open data portals).

Saves raw JSON responses as CSVs to data/raw/.
"""

import requests
import pandas as pd
import os
import time

# ── Config ────────────────────────────────────────────────────────────────────

BASE_URL = "https://data.austintexas.gov/resource"
ENDPOINTS = {
    "intakes":  "wter-evkm.json",   # Austin Animal Center Intakes
    "outcomes": "9t4d-g238.json",   # Austin Animal Center Outcomes
}

RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
PAGE_SIZE = 5000   # Socrata max per request
MAX_RECORDS = None  # Cap for dev; set to None for full pull


# ── Helpers ───────────────────────────────────────────────────────────────────

def fetch_all(endpoint_url: str, max_records: int | None = None) -> pd.DataFrame:
    """
    Paginate through a Socrata endpoint and return all records as a DataFrame.
    Socrata uses $limit / $offset for pagination.
    """
    records = []
    offset = 0

    print(f"  Fetching from: {endpoint_url}")

    while True:
        params = {
            "$limit":  PAGE_SIZE,
            "$offset": offset,
            "$order":  ":id",       # stable ordering for consistent pagination
        }

        response = requests.get(endpoint_url, params=params, timeout=30)
        response.raise_for_status()

        batch = response.json()
        if not batch:
            break

        records.extend(batch)
        offset += len(batch)
        print(f"    Fetched {offset:,} records so far...")

        if len(batch) < PAGE_SIZE:
            break  # Last page
        if max_records and offset >= max_records:
            print(f"    Reached MAX_RECORDS cap ({max_records:,}). Stopping.")
            break

        time.sleep(0.2)  # Be polite to the API

    return pd.DataFrame(records)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    os.makedirs(RAW_DIR, exist_ok=True)

    for name, endpoint in ENDPOINTS.items():
        print(f"\n[{name.upper()}]")
        url = f"{BASE_URL}/{endpoint}"
        df = fetch_all(url, max_records=MAX_RECORDS)

        out_path = os.path.join(RAW_DIR, f"{name}_raw.csv")
        df.to_csv(out_path, index=False)
        print(f"  Saved {len(df):,} rows → {out_path}")

    print("\n✅ Ingestion complete.")


if __name__ == "__main__":
    main()
