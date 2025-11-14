import os
import time
import requests
import psycopg2
from datetime import datetime

# Load environment variables from Render
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

# Extract your actual Postgres connection string
# Supabase URL looks like: https://xxxx.supabase.co
# Postgres URL is: https://xxxx.supabase.co/postrest/v1

# Your Supabase Postgres connection format:
DATABASE_HOST = SUPABASE_URL.replace("https://", "").replace(".supabase.co", ".supabase.co")
DATABASE_USER = "postgres"
DATABASE_PASSWORD = SUPABASE_SERVICE_ROLE_KEY
DATABASE_NAME = "postgres"

conn = psycopg2.connect(
    host=f"{DATABASE_HOST}",
    database=DATABASE_NAME,
    user=DATABASE_USER,
    password=DATABASE_PASSWORD,
    port="5432"
)

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

        if "results" not in data or data["results"] is None:
            return None

        price = data["results"]["p"]     # last trade price
        timestamp_ms = data["results"]["t"]
        timestamp = datetime.fromtimestamp(timestamp_ms / 1000)

        return price, timestamp

    except Exception as e:
        print(f"[ERROR] Polygon error for {symbol}: {e}")
        return None


def insert_price(symbol, price, ts):
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
        print(f"[DB ERROR] Failed insert for {symbol}: {e}")
        conn.rollback()


def main():
    print("🚀 Ingestion worker started.")
    tickers = get_tickers()
    print(f"Loaded {len(tickers)} tickers.")

    while True:
        for symbol in tickers:
            result = fetch_polygon_price(symbol)

            if result is None:
                continue

            price, ts = result
            insert_price(symbol, price, ts)

        print("Tick batch completed.")
        time.sleep(1)  # run every second


if __name__ == "__main__":
    main()
