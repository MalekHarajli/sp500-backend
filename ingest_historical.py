"""
Historical OHLC Ingestion (5 Years, 1-Minute, All S&P500 Stocks)
Batching Mode: Year-by-Year (selected by user)

Requires:
- minute_ohlc table
- SupabaseClient() class with bulk_insert()

Run once on Render or locally, then disable.
"""

import datetime
import time
from polygon_client import PolygonClient
from supabase_client import SupabaseClient


# Number of years to pull
YEARS_BACK = 5

# Polygon 1-minute candle params
MULTIPLIER = 1
TIMESPAN = "minute"


def generate_year_ranges():
    """Create YYYY-01-01 → YYYY-12-31 ranges for past N years."""
    current_year = datetime.datetime.utcnow().year
    ranges = []
    for i in range(YEARS_BACK):
        y = current_year - i - 1
        start = f"{y}-01-01"
        end = f"{y}-12-31"
        ranges.append((start, end))
    return ranges


def fetch_and_store(symbol: str, db: SupabaseClient, polygon: PolygonClient):
    """Ingest full OHLC for a ticker year-by-year."""
    print(f"\n📡 Fetching: {symbol}")

    year_ranges = generate_year_ranges()

    for start, end in year_ranges:
        print(f"   ⏳ {symbol}: {start} → {end}")

        try:
            data = polygon.get_agg(symbol, MULTIPLIER, TIMESPAN, start, end)
        except Exception as e:
            print(f"   ❌ Polygon fetch failed: {symbol} [{start} → {end}] | {e}")
            continue

        results = data.get("results", [])
        if not results:
            print(f"   ⚠ No data returned for this range.")
            continue

        rows = []
        for candle in results:
            ts = datetime.datetime.utcfromtimestamp(candle["t"] / 1000)

            rows.append({
                "symbol": symbol,
                "ts": ts,
                "open": candle["o"],
                "high": candle["h"],
                "low": candle["l"],
                "close": candle["c"],
                "volume": candle.get("v")
            })

        # INSERT INTO minute_ohlc (the correct table name)
        db.bulk_insert("minute_ohlc", rows)
        print(f"   ✅ Inserted {len(rows):,} rows")

        # Prevent Polygon rate limit
        time.sleep(0.6)


def run_ingestion():
    print("\n🚀 Starting 1-Minute Historical Ingestion (5-Year, Year-by-Year Mode)")
    polygon = PolygonClient()
    db = SupabaseClient()

    tickers = db.fetch_companies()
    tickers = [t["symbol"] for t in tickers]

    print(f"📈 Tickers loaded for ingestion: {len(tickers)} symbols")

    for symbol in tickers:
        fetch_and_store(symbol, db, polygon)

    db.close()
    print("\n🎉 Historical ingestion complete!")


if __name__ == "__main__":
    run_ingestion()


