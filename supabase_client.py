import os
import psycopg2
import psycopg2.extras
from supabase import create_client, Client
from config import DATABASE_URL


class SupabaseClient:

    def __init__(self) -> None:
        # ---- Supabase REST client (optional, not used for ingestion speed) ----
        self.supabase: Client = create_client(
            os.getenv("SUPABASE_URL", ""),
            os.getenv("SUPABASE_ANON_KEY", "")
        )

        # ---- Direct Postgres connection (fast ingestion) ----
        self.conn = psycopg2.connect(DATABASE_URL)
        self.conn.autocommit = True
        self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)


    # ======================================================
    # Fetch list of S&P 500 tickers
    # ======================================================
    def fetch_companies(self):
        self.cur.execute("SELECT symbol FROM companies ORDER BY symbol ASC;")
        return self.cur.fetchall()


    # ======================================================
    # Insert or update (upsert) a single OHLCV row
    # ======================================================
    def insert_candle(self, table: str, row: dict):
        sql = f"""
            INSERT INTO {table} (symbol, ts, open, high, low, close, volume, trade_count, vwap)
            VALUES (%(symbol)s, %(ts)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s, %(trade_count)s, %(vwap)s)
            ON CONFLICT (symbol, ts) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                trade_count = EXCLUDED.trade_count,
                vwap = EXCLUDED.vwap;
        """
        self.cur.execute(sql, row)


    # ======================================================
    # FAST bulk upsert for historical ingestion
    # ======================================================
    def bulk_insert(self, table: str, rows: list):

        if not rows:
            return

        sql = f"""
            INSERT INTO {table} (symbol, ts, open, high, low, close, volume, trade_count, vwap)
            VALUES (%(symbol)s, %(ts)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s, %(trade_count)s, %(vwap)s)
            ON CONFLICT (symbol, ts) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                trade_count = EXCLUDED.trade_count,
                vwap = EXCLUDED.vwap;
        """

        try:
            psycopg2.extras.execute_batch(self.cur, sql, rows, page_size=500)
        except Exception as e:
            print(f"❌ Bulk insert failed for {table}: {e}")


    # ======================================================
    # Close DB connection
    # ======================================================
    def close(self):
        try:
            self.cur.close()
            self.conn.close()
        except Exception:
            pass
