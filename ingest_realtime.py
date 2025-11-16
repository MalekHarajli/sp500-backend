import time
from polygon_client import PolygonClient
from supabase_client import SupabaseClient
from calculate_index import compute_index
from calculate_contributions import compute_contributions

polygon = PolygonClient()
db = SupabaseClient()

def run_realtime():
    while True:
        # Fetch snapshot data
        data = polygon.get_all_snapshots()
        results = data.get("tickers", []) if isinstance(data, dict) else []

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
            db.insert_second_prices(rows)

            compute_index()
            compute_contributions()

        time.sleep(1)

if __name__ == "__main__":
    run_realtime()
