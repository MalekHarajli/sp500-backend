import time
from datetime import datetime, timedelta
from polygon_client import PolygonClient
from supabase_client import SupabaseClient

def run_historical_ingest():
    polygon = PolygonClient()
    supa = SupabaseClient()

    # 2 years of data
    end = datetime.utcnow()
    start = end - timedelta(days=730)

    # Supabase companies table
    companies = supa.fetch_companies()

    timeframes = ["1", "5", "15", "60", "240", "D", "W"]

    for (symbol,) in companies:
        print(f"[+] {symbol}")

        for tf in timeframes:
            print(f"    → timeframe {tf}")

            candles = polygon.fetch_aggregates(symbol, tf, start, end)
            if not candles:
                print(f"    (warning: no data for {symbol} tf={tf})")
                continue

            print(f"    inserting {len(candles)} rows...")
            supa.insert_historical_candles(symbol, tf, candles)

            time.sleep(0.25)

    print("Historical ingestion complete!")

if __name__ == "__main__":
    run_historical_ingest()
