import time
from datetime import datetime
from polygon_client import PolygonClient
from supabase_client import SupabaseClient
from calculate_index import compute_index
from calculate_contributions import compute_contributions

polygon = PolygonClient()
db = SupabaseClient()

def run_realtime():
    while True:
        data = polygon.get_all_snapshots()
        tickers = data.get("tickers", []) if isinstance(data, dict) else []

        rows = []

        for t in tickers:
            sym = t.get("ticker")
            trade = t.get("lastTrade")
            if not trade:
                continue

            price = trade.get("p")
            ts_micro = trade.get("t")

            if not price or not ts_micro:
                continue

            dt = datetime.utcfromtimestamp(ts_micro / 1_000_000)
            rows.append((sym, dt, price, None))

        if rows:
            db.insert_second_prices(rows)
            compute_index()
            compute_contributions()

        time.sleep(1)

if __name__ == "__main__":
    run_realtime()
