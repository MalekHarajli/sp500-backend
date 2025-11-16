import psycopg2
from psycopg2.extras import execute_values
from config import DATABASE_URL

def get_connection():
    return psycopg2.connect(DATABASE_URL)

class SupabaseClient:
    def __init__(self):
        self.conn = get_connection()
        self.conn.autocommit = True

    def insert_second_prices(self, rows):
        with self.conn.cursor() as cur:
            query = """
                INSERT INTO second_prices (symbol, ts, price, volume)
                VALUES %s
                ON CONFLICT (symbol, ts) DO NOTHING;
            """
            execute_values(cur, query, rows)

    def insert_index_value(self, ts, value):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO sp500_index_values (ts, index_value)
                VALUES (%s, %s)
                ON CONFLICT (ts) DO NOTHING;
            """, (ts, value))

    def insert_contributions(self, rows):
        with self.conn.cursor() as cur:
            query = """
                INSERT INTO contributions_live (ts, symbol, contribution_pct)
                VALUES %s;
            """
            execute_values(cur, query, rows)

    def get_weights(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT symbol, weight FROM sp500_weights;")
            return cur.fetchall()
