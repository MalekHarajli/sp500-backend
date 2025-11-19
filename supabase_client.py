import os
import psycopg2
import psycopg2.extras
from config import DATABASE_URL


# ================================================
# SAFE TABLE WHITELIST  (prevents SQL injection)
# ================================================
VALID_TABLES = {
    "minute_ohlc",
    "ohlc_5m",
    "ohlc_15m",
    "ohlc_1h",
    "ohlc_4h",
    "ohlc_1d",
    "second_prices",
    "companies"
}


class SupabaseClient:

    def __init__(self) -> None:
        if not DATABASE_URL:
            raise RuntimeError("❌ DATABASE_URL not found — ensure it is set in Render")

        self.conn = psycopg2.connect(DATABASE_URL)
        self.conn.autocommit = True
        self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)


    # ================================================
    # Ensure table name is valid
    # ================================================
    def _validate_table(self, table: str):
        if table not in VALID_TABLES:
            raise ValueError(f"❌ INVALID TABLE NAME: {table}")


    # ================================================
    # Fetch S&P 500 tickers
    # ================================================
    def fetch_companies(self):
        self.cur.execute("SELECT symbol FROM companies ORDER BY symbol ASC;")
        return self.cur.fetchall()


    # ================================================
    # Insert or update one candle row
    # ================================================
    def insert_candle(self, table: str, row: dict):

        self._validate_table(table)

        sql = f"""
            INSERT INTO {table} 
                (symbol, ts, open, high, low, close, volume)
            VALUES 
                (%(symbol)s, %(ts)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s)
            ON CONFLICT (symbol, ts) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume;
        """

        try:
            self.cur.execute(sql, row)
        except Exception as e:
            print(f"❌ insert_candle failed: {e} | Row: {row}")


    # ================================================
    # Bulk upsert for historical ingestion
    # ================================================
    def bulk_insert(self, table: str, rows: list):

        self._validate_table(table)

        if not rows:
            print(f"⚠ No rows passed to bulk_insert for {table}")
            return

        sql = f"""
            INSERT INTO {table} 
                (symbol, ts, open, high, low, close, volume)
            VALUES 
                (%(symbol)s, %(ts)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s)
            ON CONFLICT (symbol, ts) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume;
        """

        try:
            psycopg2.extras.execute_batch(self.cur, sql, rows, page_size=1000)
            print(f"✅ Inserted {len(rows):,} rows into {table}")
        except Exception as e:
            print(f"❌ bulk_insert failed for {table}: {e}")


    # ================================================
    # Close connection
    # ================================================
    def close(self):
        try:
            self.cur.close()
            self.conn.close()
        except:
            pass
