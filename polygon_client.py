import requests
import backoff
from config import POLYGON_API_KEY

BASE_URL = "https://api.polygon.io"


class PolygonClient:

    def __init__(self, api_key: str = POLYGON_API_KEY):
        if not api_key or api_key == "YOUR_POLYGON_KEY_HERE":
            raise ValueError("Polygon API key missing. Please set POLYGON_API_KEY in config.py or environment.")
        self.api_key = api_key

    @backoff.on_exception(backoff.expo, Exception, max_tries=5)
    def get_agg(self, symbol: str, multiplier: int, timespan: str, start: str, end: str):
        """
        Generic Polygon aggregate function
        Example:
            1-min:  multiplier=1, timespan="minute"
            5-min: multiplier=5, timespan="minute"
            1-hr:  multiplier=1, timespan="hour"
            Daily: multiplier=1, timespan="day"
        """
        url = f"{BASE_URL}/v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{start}/{end}"
        params = {
            "adjusted": "true",
            "sort": "asc",
            "limit": 50000,
            "apiKey": self.api_key,
        }
        r = requests.get(url, params=params)
        r.raise_for_status()
        return r.json()

    @backoff.on_exception(backoff.expo, Exception, max_tries=5)
    def get_previous_close(self, symbol: str):
        url = f"{BASE_URL}/v2/aggs/ticker/{symbol}/prev"
        params = {"adjusted": "true", "apiKey": self.api_key}
        r = requests.get(url, params=params)
        r.raise_for_status()
        return r.json()

    @backoff.on_exception(backoff.expo, Exception, max_tries=5)
    def get_all_snapshots(self):
        """
        REAL-TIME SNAPSHOT OF ALL US STOCK TICKERS
        Docs: https://polygon.io/docs/stocks/get_v2_snapshot_locale_us_markets_stocks_tickers
        """
        url = f"{BASE_URL}/v2/snapshot/locale/us/markets/stocks/tickers"
        params = {"apiKey": self.api_key}

        r = requests.get(url, params=params)
        r.raise_for_status()
        data = r.json()

        # Return only ticker list
        return data.get("tickers", []) if isinstance(data, dict) else []
