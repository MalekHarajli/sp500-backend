import requests
import backoff
from config import POLYGON_API_KEY

BASE_URL = "https://api.polygon.io"


class PolygonClient:

    def __init__(self, api_key: str = POLYGON_API_KEY):
        self.api_key = api_key

    # -------------------------
    # 🔹 REAL-TIME SNAPSHOTS
    # -------------------------
    @backoff.on_exception(backoff.expo, Exception, max_tries=5)
    def get_all_snapshots(self):
        """Returns real-time snapshot data for all tradable US stocks."""
        url = f"{BASE_URL}/v3/snapshot/stocks"
        headers = {"Authorization": f"Bearer {self.api_key}"}

        r = requests.get(url, headers=headers)
        r.raise_for_status()
        data = r.json()

        return data.get("tickers", [])  # always return a list

    # -------------------------
    # 🔹 HISTORICAL AGGREGATES
    # -------------------------
    @backoff.on_exception(backoff.expo, Exception, max_tries=5)
    def get_agg(self, symbol: str, multiplier: int, timespan: str, start: str, end: str):
        """
        Generic function for Polygon aggregates.
        Example:
        - 1 min: multiplier=1, timespan="minute"
        - 5 min: multiplier=5, timespan="minute"
        - 1 hour: multiplier=1, timespan="hour"
        - Day: multiplier=1, timespan="day"
        """
        url = f"{BASE_URL}/v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{start}/{end}"
        params = {
            "adjusted": "true",
            "sort": "asc",
            "limit": 50000,
            "apiKey": self.api_key
        }

        r = requests.get(url, params=params)
        r.raise_for_status()
        return r.json()

    # -------------------------
    # 🔹 PREVIOUS CLOSE
    # -------------------------
    @backoff.on_exception(backoff.expo, Exception, max_tries=5)
    def get_previous_close(self, symbol: str):
        url = f"{BASE_URL}/v2/aggs/ticker/{symbol}/prev"
        params = {"adjusted": "true", "apiKey": self.api_key}

        r = requests.get(url, params=params)
        r.raise_for_status()
        return r.json()
