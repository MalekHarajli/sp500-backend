import time
import datetime
from polygon_client import PolygonClient
from supabase_client import SupabaseClient
from calculate_index import compute_index
from calculate_contributions import compute_contributions

polygon = PolygonClient()
db = SupabaseClient()

def run_realtime():
    print("🔥 15-Minute Delayed S&P500 Feed Started (1-minute refresh)")
    last_timestamp = None

    while True:
        # Fetch current date (Polygon requires YYYY-MM-DD)
        today_str = datetime.datetime.utcnow().strftime("%Y-%m-%d")

        print(f"\n⏳ Fetching grouped aggregates for {today_str} ...")
        data = polygon.get_grouped_aggregates(today_str)

        if not isinstance(data, dict) or "results" not in data:
            print("⚠ No results found, sleeping for 60s...")
            time.sleep(60)
            continue

        results = data["results"]
        print(f"📈 Received {len(results)} tickers")

        rows = []

        for row in results:
            symbol = row.get("T")
            price = row.get("c")
            ts_ms = row.get("t")  # ms timestamp

            if not symbol or price is None or ts_ms is None:
                continue

            ts = datetime.datetime.utcfromtimestamp(ts_ms / 1000)

            # Avoid inserting duplicates
            if last_timestamp and ts <= last_timestamp:
                continue

            rows.append((symbol, ts, price, row.get("v")))

        if rows:
            print(f"💾 Inserting {len(rows)} new price rows")
            db.insert_second_prices(rows)

            print("📐 Updating S&P500 index value...")
            compute_index()

            print("📊 Updating contribution breakdown...")
            compute_contributions()

            last_timestamp = rows[-1][1]
        else:
            print("ℹ No new rows detected (waiting for next minute)")

        time.sleep(60)

if __name__ == "__main__":
    run_realtime()
