import time
from polygon_client import PolygonClient
from supabase_client import SupabaseClient
from calculate_index import compute_index
from calculate_contributions import compute_contributions

polygon = PolygonClient()

def run_realtime():
    print("🔥 Realtime ingestion started...")

    while True:
        try:
            print("⏳ Fetching snapshot data...")
            data = polygon.get_all_snapshots()

            # Debug print
            print(f"📊 Snapshot response type: {type(data)}")

            results = data.get("tickers", []) if isinstance(data, dict) else []
            print(f"📈 Tickers received: {len(results)}")

            if len(results) == 0:
                print("⚠ No data returned from Polygon, retrying in 3 seconds...")
                time.sleep(3)
                continue

            db = SupabaseClient()
            rows = []

            for t in results:
                sym = t.get("ticker")
                last_trade = t.get("lastTrade")

                if not last_trade:
                    continue

                price = last_trade.get("p")
                ts_micro = last_trade.get("t")

                if price is None or ts_micro is None:
                    continue

                import datetime
                dt = datetime.datetime.utcfromtimestamp(ts_micro / 1_000_000)

                rows.append((sym, dt, price, None))

            if rows:
                print(f"📝 Inserting {len(rows)} rows into second_prices...")
                db.insert_second_prices(rows)

                print("🧮 Computing index...")
                compute_index()

                print("📊 Computing contributions...")
                compute_contributions()

                print("✅ Cycle complete.")
            else:
                print("⚠ No valid rows produced from snapshot.")

            time.sleep(1)

        except Exception as e:
            print(f"❌ ERROR: {e}")
            time.sleep(3)

if __name__ == "__main__":
    run_realtime()
