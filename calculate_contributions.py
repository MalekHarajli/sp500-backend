from supabase_client import get_connection

def compute_contributions():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT w.symbol, w.weight, p.price
        FROM sp500_weights w
        JOIN LATERAL (
            SELECT price FROM second_prices
            WHERE symbol=w.symbol
            ORDER BY ts DESC
            LIMIT 2
        ) p ON TRUE
        ORDER BY w.symbol, p.price DESC
    """)

    rows = cur.fetchall()

    contributions = []

    for i in range(0, len(rows), 2):
        symbol, weight, current = rows[i]
        _, _, previous = rows[i+1]

        delta = current - previous if previous else 0
        contribution = weight * delta
        contributions.append((symbol, contribution))

    for symbol, contribution in contributions:
        cur.execute("""
            INSERT INTO contributions_live (ts, symbol, contribution_pct)
            VALUES (NOW(), %s, %s)
        """, (symbol, contribution))

    conn.commit()
    cur.close()
    conn.close()
