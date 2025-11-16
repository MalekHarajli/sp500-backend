from supabase_client import SupabaseClient

def compute_index():
    db = SupabaseClient()
    conn = db.get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT w.symbol, w.weight, p.price
        FROM sp500_weights w
        JOIN LATERAL (
            SELECT price FROM second_prices
            WHERE symbol = w.symbol
            ORDER BY ts DESC
            LIMIT 1
        ) p ON TRUE;
    """)

    rows = cur.fetchall()
    index_value = sum(weight * price for (_, weight, price) in rows)

    cur.execute("""
        INSERT INTO sp500_index_values (ts, index_value)
        VALUES (NOW(), %s)
        ON CONFLICT (ts) DO UPDATE SET index_value = EXCLUDED.index_value;
    """, (index_value,))

    conn.commit()
    cur.close()
    db.close()
