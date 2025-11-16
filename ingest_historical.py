import datetime
import time
import pytz
from polygon_client import PolygonClient
from supabase_client import SupabaseClient

# Historical range: last 5 years from today
YEARS_BACK = 5
MAX_DAYS_PER_REQUEST = 30   # Polygon allows date ranges, so chunking reduces # of calls


def daterange(start_date, end_date, step_days):
    while start_date < end_date:
        chunk_end = min(start_date + datetime.timedelta(days=step_days), end_date)
        yield start_date, chunk_end
        start_date = chunk_end


def run_historical():
    print("🚀 Starting Historical Ingestion...")
    db = SupabaseClient()
    polygon = PolygonClient()

    # Get ticker list
    tickers = [row["symbol"] for row in db.fetch_companies()]
    print(f"📈 Found {len(tickers)} tickers to ingest")

    utc = pytz.UTC
    eastern = pytz.timezone("US/Eastern")
    end_date = datetime.datetime.now(utc)
    start_date = end_date - datetime.timedelta(days=YEARS_BACK * 365)

    for symbol in tickers:
        print(f"\n============================")
        print(f"📊 Processing: {symbol}")
        print(f"============================")

        # 1️⃣ Read checkpoint
        checkpoint = db.get_checkpoint(symbol)
        if checkpoint:
            print(f"⏩ Resuming from checkpoint: {checkpoint}")
            start = checkpoint
        else:
            print("🔰 No checkpoint found — starting from 5-year baseline")
            start = start_date

        # 2️⃣ Loop in chunks
        for chunk_start, chunk_end in daterange(start, end_date, MAX_DAYS_PER_REQUEST):
            print(f"  ⏳ Fetching {chunk_start.date()} → {chunk_end.date()}")

            data = polygon.get_agg(
                symbol=symbol,
                multiplier=1,
                timespan="minute",
                start=chunk_start.strftime("%Y-%m-%d"),
                end=chunk_end.strftime("%Y-%m-%d")
            )

            results = data.get("results", [])
            if not results:
                print("  ⚠ No data found — continuing...")
                continue

            rows = []
            for r in results:
                ts = datetime.datetime.fromtimestamp(r["t"] / 1000, utc).astimezone(eastern)

                rows.append({
                    "symbol": symbol,
                    "ts": ts,
                    "open": r.get("o"),
                    "high": r.get("h"),
                    "low": r.get("l"),
                    "close": r.get("c"),
                    "volume": r.get("v", 0),
                    "trade_count": r.get("n", 0),
                    "vwap": r.get("vw")
                })

            db.bulk_insert("minute_ohlcv", rows)

            # Update checkpoint
            db.update_checkpoint(symbol, chunk_end)

            print(f"  ✅ Inserted {len(rows)} rows")
            time.sleep(1.2)  # rate limit safety

    db.close()
    print("\n🎉 Historical ingestion complete!")
            

if __name__ == "__main__":
    run_historical()

