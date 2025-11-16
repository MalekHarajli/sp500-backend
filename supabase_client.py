# supabase_client.py

import os
import psycopg2
import psycopg2.extras
from supabase import create_client, Client
from config import DATABASE_URL


class SupabaseClient:
    """
    Hybrid DB access layer:
      - Direct Postgres (psycopg2) for high-frequency ingestion (fast, safe, bulk)
      - Supabase REST client for future authenticated UI / API usage
    """

    def __init__(self) -> None:
        # ---- Validate required env ----
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_ANON_KEY")

        if not DATABASE_URL:
            raise ValueError("❌ DATABASE_URL missing. Check Render & local .env")

        # ---- Init optional Supabase REST client ----
        self.supabase: Client | None = None
        if supabase_url and supabase_key:
            self.supabase = create_client(supabase_url, supabase_key)

        # ---- Direct Postgres connection ----
        self.conn = psycopg2.connect(DATABASE_URL)
        self.conn.autocommit = True
        self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        print("🚀 SupabaseClient initialized (Hybrid Mode Active)")


    # ================================
    # Fetch ticker list (S&P 500)
    # ================================
    def fetch_companies(self) -> list[str]:
        self.cur.execute("SELECT symbol FROM companies ORDER BY symbol ASC;")
        rows = self.cur.fetchall()
        return [row["symbol"] for row in rows]


    # ================================
    # Insert 1 row (live ingestion)
    # ================================
    def insert_candle(self, table: str, row: dict):
        sql = f"""
            INSERT INTO {table} (symbol, ts, open, high, low, close, volume)
            VALUES (%(symbol)s, %(ts)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s)
            ON CONFLICT (symbol, ts) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume;
        """
        self.cur.execute(sql, row)


    # ================================
    # Bulk insert for historical data
    # ================================
    def bulk_insert(self, table: str, rows: list[dict]):
        if not rows:
            return

        sql = f"""
            INSERT INTO {table} (symbol, ts, open, high, low, close, volume)
            VALUES (%(symbol)s, %(ts)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s)
            ON CONFLICT (symbol, ts) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume;
        """

        try:
            psycopg2.extras.execute_batch(self.cur, sql, rows, page_size=500)
        except Exception as e:
            print(f"❌ Bulk insert failed for {table}: {e}")
        else:
            print(f"✔ Inserted/updated {len(rows)} rows in {table}")


    # ================================
    # Close connection safely
    # ================================
    def close(self):
        try:
            self.cur.close()
            self.conn.close()
        except:
            pass

