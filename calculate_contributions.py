from supabase_client import SupabaseClient
from datetime import datetime

db = SupabaseClient()

def compute_contributions():
    weights = dict(db.get_weights())  # {symbol: weight}

    rows = []
    with db.conn.cursor() as cur:
        cur.execute("""
            SELECT symbol, price
            FROM public.second_prices
            WHERE ts IN (
                SELECT DISTINCT ts FROM public.second_prices ORDER BY ts DESC LIMIT 2
            )
            ORDER BY symbol, ts DESC
        """)
        rows = cur.fetchall()

    # Build map: {symbol: [currentPrice, prevPrice]}
    price_map = {}
    for symbol, price in rows:
        price_map.setdefault(symbol, []).append(price)

    contributions = []
    for symbol, price_list in price_map.items():
        if len(price_list) == 2 and symbol in weights:
            delta = float(price_list[0]) - float(price_list[1])
            contributions.append((datetime.utcnow(), symbol, float(weights[symbol]) * delta))

    if contributions:
        db.insert_contributions(contributions)
