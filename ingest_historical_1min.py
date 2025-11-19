import datetime
from polygon_client import PolygonClient
from supabase_client import SupabaseClient

# how many years back to load
YEARS_BACK = 5


def daterange_chunks(start_date, end_date, chunk_days=31):
    """Yield start and end dates in small chunks for API safety."""
    current = start_date
    while current < end_date:
        chunk_end = min(current + datetime.timedelta(days=chunk_days), end_date)
        yield current, chunk_end
        current = chunk_end


def run_historical_ingestion():
    pc = PolygonClient()
    db = SupabaseClient()

    print("📌 Starting 1-minute historical ingestion...")

    # Fetch all ticker symbols from companies table
    tickers = db.fetch_companies()
    tickers = [t['symbol'] for t in tickers]

    # date range
    end_date = datetime.datetime.utcnow().date()
    start_date = end_date - datetime.timedelta(days=YEARS_BACK * 365)

    for symbol in tickers:
        print(f"\n📈 Fetching history for: {symbol}")

        for start, end in daterange_chunks(start_date, end_date, chunk_days=30):
            # Convert to YYYY-MM-DD format
            s = start.strftime("%Y-%m-%d")
            e = end.strftime("%Y-%m-%d")

            print(f"   ⏳ {symbol}: {s} → {e}")

            try:
                data = pc.get_agg(symbol, multiplier=1, timespan="minute", start=s, end=e)

                # extract candles
                results = data.get("results", []) if isinstance(data, dict) else []

                rows = []
                for c in results:
                    ts = datetime.datetime.utcfromtimestamp(c["t"] / 1000)
                    rows.append({
                        "symbol": symbol,
                        "ts": ts,
                        "open": c.get("o"),
                        "high": c.get("h"),
                        "low": c.get("l"),
                        "close": c.get("c"),
                        "volume": c.get("v"),
                        "trade_count": c.get("n"),
                        "vwap": c.get("vw")
                    })

                if rows:
                    # FIXED TABLE NAME
                    db.bulk_insert("minute_ohlc", rows)
                    print(f"   ✅ Stored {len(rows)} rows")

            except Exception as e:
                print(f"   ❌ Error for {symbol} in chunk {s} → {e}")

    db.close()
    print("\n🎉 Historical ingestion finished!")


if __name__ == "__main__":
    run_historical_ingestion()
