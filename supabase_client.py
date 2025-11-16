# supabase_client.py

import psycopg2
from psycopg2.extras import execute_values
from config import DATABASE_URL


class SupabaseClient:
    def __init__(self):
        self.conn = psycopg2.connect(DATABASE_URL)
        self.conn.autocommit = True

    def insert_second_prices(self, rows):
        """
        rows = [
            (symbol, ts, price, volume),
            ...
        ]
        """
        with self.conn.cursor() as cur:
            query = """
                INSERT INTO public.second_prices (symbol, ts, price, volume)
                VALUES %s
                ON CONFLICT (symbol, ts) DO UPDATE
                SET price = EXCLUDED.price,
                    volume = EXCLUDED.volume;
            """
            execute_values(cur, query, rows)

    def insert_index_value(self, ts, value):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.sp500_index_values (ts, index_value)
                VALUES (%s, %s)
                ON CONFLICT (ts) DO UPDATE
                SET index_value = EXCLUDED.index_value;
                """,
                (ts, value),
            )

    def insert_contributions(self, rows):
        """
        rows = [
            (ts, symbol, contribution_pct),
            ...
        ]
        """
        with self.conn.cursor() as cur:
            query = """
                INSERT INTO public.contributions_live (ts, symbol, contribution_pct)
                VALUES %s
                ON CONFLICT DO NOTHING;
            """
            execute_values(cur, query, rows)

    def get_weights(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT symbol, weight FROM public.sp500_weights;")
            return cur.fetchall()

    def close(self):
        self.conn.close()

