from supabase_client import SupabaseClient
from datetime import datetime

db = SupabaseClient()

def compute_index():
    rows = db.get_weights()  # returns: [(symbol, weight), ...]

    if not rows:
        return
    
    index_value = 0.0

    with db.conn.cursor() as cur:
        for symbol, weight in rows:
            cur.execute("""
                SELECT price FROM public.second_prices
                WHERE symbol = %s
                ORDER BY ts DESC
                LIMIT 1
            """, (symbol,))
            result = cur.fetchone()
            if result and result[0] is not None:
                index_value += float(weight) * float(result[0])

    db.insert_index_value(datetime.utcnow(), index_value)
