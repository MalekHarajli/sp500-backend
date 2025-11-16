import datetime
from polygon_client import PolygonClient
from supabase_client import SupabaseClient
from time import sleep

# timeframes mapped to db table name + polygon params
TIMEFRAMES = {
    "1m":  ("prices_1m", 1,  "minute"),
    "5m":  ("prices_5m", 5,  "minute"),
    "15m": ("prices_15m", 15, "minute"),
    "1h":  ("prices_1h", 1,  "hour"),
    "4h":  ("prices_4h", 4,  "hour"),
    "1d":  ("prices_1d", 1,  "day"),
}

# how many years back we fetch
YEARS_BACK = 5


def ingest_historical():
    print("🚀 Starting historical OHLCV ingestion...\n")

    db = SupabaseClient()
    polygon = PolygonClient()

    # Fetch ticker list from DB
    tickers = db.fetch_companies()
    print(f"📈 Loaded {len(tickers)} tickers.")

    end_date = datetime.datetime.utcnow().date()
    start_date = end_date - datetime.timedelta(days=365 * YEARS_BACK)

    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    for timeframe, (table, multiplier, granularity) in TIMEFRAMES.items():
        print(f"\n========== ⏱ {timeframe} candles → {table} ==========")

        for symbol in tickers:
            symbol = symbol["symbol"]
            try:
                print(f"📥 Fetching {symbol} ({timeframe})")

                data = polygon.get_agg(
                    symbol=symbol,
                    multiplier=multiplier,
                    timespan=granularity,
                    start=start_str,
                    end=end_str
                )

                results = data.get("results", [])
                if not results:
                    print(f"⚠ No results for {symbol}")
                    continue

                rows_to_insert = []
                for candle in results:
                    rows_to_insert.append({
                        "symbol": symbol,
                        "ts": datetime.datetime.utcfromtimestamp(candle["t"] / 1000.0).isoformat(),
                        "open": candle["o"],
                        "high": candle["h"],
                        "low": candle["l"],
                        "close": candle["c"],
                        "volume": candle.get("v")
                    })

                db.bulk_insert(table, rows_to_insert)
                print(f"✔ {symbol} -> inserted {len(rows_to_insert)} rows")

                sleep(0.3)  # rate limit protection

            except Exception as e:
                print(f"❌ Error on {symbol}: {e}")
                continue

    print("\n🎉 Historical ingestion complete!")



if __name__ == "__main__":
    ingest_historical()
