import time
from polygon_client import get_all_snapshots
from supabase_client import insert_many
from calculate_index import compute_index
from calculate_contributions import compute_contributions

def run_realtime():
    while True:
        data = get_all_snapshots()
        results = data.get("tickers", [])

        rows = []
        now = None

        for t in results:
            sym = t["ticker"]
            if "lastTrade" not in t:
                continue

            price = t["lastTrade"]["p"]
            ts = t["lastTrade"]["t"] / 1000 / 1000  # convert micros → seconds

            import datetime
            dt = datetime.datetime.utcfromtimestamp(ts)

            rows.append((sym, dt, price, None))

        if rows:
            insert_many("""
                INSERT INTO second_prices (symbol, ts, price, volume)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (symbol, ts) DO NOTHING
            """, rows)

            compute_index()
            compute_contributions()

        time.sleep(1)

if __name__ == "__main__":
    run_realtime()
