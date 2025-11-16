import os
import psycopg2
import psycopg2.extras
from config import DATABASE_URL


class SupabaseClient:

    def __init__(self) -> None:
        # Fast Postgres connection for ingestion & reads
        self.conn = psycopg2.connect(DATABASE_URL)
        self.conn.autocommit = True
        self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # ------------------------------------------------------
    # Fetch list of S&P 500 tickers from companies table
    # ------------------------------------------------------
    def fetch_companies(self):
        self.cur.execute("SELECT symbol FROM companies ORDER BY symbol ASC;")
        return self.cur.fetchall()

    # ------------------------------------------------------
    # Insert or update single OHLCV candle record
    # (Used for live updates)
    # ------------------------------------------------------
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

    # ------------------------------------------------------
    # BULK UPSERT for historical ingestion
    # ------------------------------------------------------
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


    # ------------------------------------------------------
    # CHECKPOINT: read last processed timestamp per symbol
    # ------------------------------------------------------
    def get_checkpoint(self, symbol: str):
        self.cur.execute("SELECT last_ts FROM ingest_checkpoints WHERE symbol = %s", (symbol,))
        row = self.cur.fetchone()
        return row["last_ts"] if row else None

    # ------------------------------------------------------
    # CHECKPOINT: update timestamp
    # ------------------------------------------------------
    def update_checkpoint(self, symbol: str, ts):
        self.cur.execute("""
            INSERT INTO ingest_checkpoints (symbol, last_ts)
            VALUES (%s, %s)
            ON CONFLICT (symbol) DO UPDATE SET last_ts = EXCLUDED.last_ts
        """, (symbol, ts))

    # ------------------------------------------------------
    # Close DB connection safely
    # ------------------------------------------------------
    def close(self):
        try:
            self.cur.close()
            self.conn.close()
        except:
            pass

