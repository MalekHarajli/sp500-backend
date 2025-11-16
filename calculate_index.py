from supabase_client import get_connection

def compute_index():
    conn = get_connection()
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
        ON CONFLICT (ts) DO NOTHING;
    """, (index_value,))

    conn.commit()
    cur.close()
    conn.close()
