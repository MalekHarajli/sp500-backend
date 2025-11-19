import time
import datetime
from polygon_client import PolygonClient
from supabase_client import SupabaseClient

polygon = PolygonClient()
db = SupabaseClient()

def run_realtime():
    print("🔥 Live S&P500 1-Minute Feed Started")
    last_timestamp = None

    while True:
        today_str = datetime.datetime.utcnow().strftime("%Y-%m-%d")
        print(f"\n⏳ Fetching grouped aggregates for {today_str} ...")

        data = polygon.get_grouped_aggregates(today_str)

        if not isinstance(data, dict) or "results" not in data:
            print("⚠ No results found, sleeping 60s...")
            time.sleep(60)
            continue

        results = data["results"]
        print(f"📈 Received {len(results)} tickers")

        inserts = []

        for row in results:
            symbol = row.get("T")
            ts_ms = row.get("t")  # timestamp (ms)
            if not symbol or ts_ms is None:
                continue

            ts = datetime.datetime.utcfromtimestamp(ts_ms / 1000)

            # Avoid duplicates
            if last_timestamp and ts <= last_timestamp:
                continue

            inserts.append({
                "symbol": symbol,
                "ts": ts,
                "open": row.get("o"),
                "high": row.get("h"),
                "low": row.get("l"),
                "close": row.get("c"),
                "volume": row.get("v"),
            })

        if inserts:
            print(f"💾 Writing {len(inserts)} new minute candles...")
            db.bulk_insert("minute_ohlc", inserts)

            last_timestamp = inserts[-1]["ts"]
        else:
            print("ℹ No new rows (sleeping)")

        time.sleep(60)


if __name__ == "__main__":
    run_realtime()
