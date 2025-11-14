import os
import time
import requests
import psycopg2
from datetime import datetime

# Environment variables
DB_HOST = os.getenv("SUPABASE_DB_HOST")  # we will set this below
DB_PASSWORD = os.getenv("SUPABASE_DB_PASSWORD")
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

# Hard-coded because Supabase always uses these
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PORT = 5432

# Connect to Supabase Postgres
def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT,
            sslmode="require"
        )
        return conn
    except Exception as e:
        print(f"[FATAL] Cannot connect to database: {e}")
        raise e

conn = get_db_connection()
cursor = conn.cursor()


def get_tickers():
    cursor.execute("SELECT symbol FROM public.companies;")
    rows = cursor.fetchall()
    return [row[0] for row in rows]


def fetch_polygon_price(symbol):
    url = f"https://api.polygon.io/v2/last/trade/{symbol}?apiKey={POLYGON_API_KEY}"
    try:
        response = requests.get(url, timeout=3)
        data = response.json()

        if "results" not in data:
            return None

        price = data["results"]["p"]
        ts = datetime.fromtimestamp(data["results"]["t"] / 1000)

        return price, ts

    except Exception as e:
        print(f"[ERROR] Polygon API error for {symbol}: {e}")
        return None


def insert_second_price(symbol, price, ts):
    try:
        cursor.execute(
            """
            INSERT INTO public.second_prices (symbol, ts, price)
            VALUES (%s, %s, %s)
            ON CONFLICT (symbol, ts) DO NOTHING;
            """,
            (symbol, ts, price)
        )
        conn.commit()
    except Exception as e:
        print(f"[DB ERROR] Insert failed for {symbol}: {e}")
        conn.rollback()


def main():
    print("🚀 Worker started!")
    tickers = get_tickers()
    print(f"Loaded {len(tickers)} tickers.")

    while True:
        for symbol in tickers:
            data = fetch_polygon_price(symbol)
            if data:
                price, ts = data
                insert_second_price(symbol, price, ts)

        print("Tick batch complete.")
        time.sleep(1)


if __name__ == "__main__":
    main()
