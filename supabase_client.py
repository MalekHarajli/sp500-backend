import psycopg2
from psycopg2.extras import execute_values
from config import DATABASE_URL

class SupabaseClient:

    def __init__(self):
        self.conn = psycopg2.connect(DATABASE_URL)
        self.conn.autocommit = True

    def get_conn(self):
        return self.conn

    def insert_second_prices(self, rows):
        with self.conn.cursor() as cur:
            query = """
                INSERT INTO second_prices (symbol, ts, price, volume)
                VALUES %s
                ON CONFLICT (symbol, ts) DO NOTHING
            """
            execute_values(cur, query, rows)

    def insert_index_value(self, ts, value):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO sp500_index_values (ts, index_value)
                VALUES (%s, %s)
                ON CONFLICT (ts) DO UPDATE SET index_value = EXCLUDED.index_value
            """, (ts, value))

    def insert_contributions(self, rows):
        with self.conn.cursor() as cur:
            query = """
                INSERT INTO contributions_live (ts, symbol, contribution_pct)
                VALUES %s
                ON CONFLICT DO NOTHING
            """
            execute_values(cur, query, rows)

    def close(self):
        self.conn.close()
