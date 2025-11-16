import requests
import backoff
from config import POLYGON_API_KEY

BASE_URL = "https://api.polygon.io"


class PolygonClient:

    def __init__(self, api_key: str = POLYGON_API_KEY):
        if not api_key or api_key.strip() == "":
            raise ValueError("Polygon API key is missing. Set POLYGON_API_KEY in your .env file.")
        self.api_key = api_key

    # ---------------------------
    # Generic OHLC / aggregate data
    # ---------------------------
    @backoff.on_exception(backoff.expo, Exception, max_tries=5)
    def get_agg(self, symbol: str, multiplier: int, timespan: str, start: str, end: str):
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

    # ---------------------------
    # Previous close endpoint
    # ---------------------------
    @backoff.on_exception(backoff.expo, Exception, max_tries=5)
    def get_previous_close(self, symbol: str):
        url = f"{BASE_URL}/v2/aggs/ticker/{symbol}/prev"
        params = {"adjusted": "true", "apiKey": self.api_key}
        r = requests.get(url, params=params)
        r.raise_for_status()
        return r.json()

    # ---------------------------
    # Real-time snapshot endpoint (used by ingest_realtime.py)
    # ---------------------------
    @backoff.on_exception(backoff.expo, Exception, max_tries=5)
    def get_all_snapshots(self):
        url = f"{BASE_URL}/v3/snapshot/tickers"
        params = {"apiKey": self.api_key}
        r = requests.get(url, params=params)
        r.raise_for_status()
        return r.json()
