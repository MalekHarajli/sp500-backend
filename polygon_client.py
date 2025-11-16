import requests
import backoff
from config import POLYGON_API_KEY

BASE_URL = "https://api.polygon.io"


class PolygonClient:

    def __init__(self, api_key: str = POLYGON_API_KEY):
        if not api_key or api_key.strip() == "":
            raise ValueError("Polygon API key missing. Ensure POLYGON_API_KEY is set in Render environment.")
        self.api_key = api_key

    @backoff.on_exception(backoff.expo, Exception, max_tries=5)
    def get_grouped_aggregates(self, date_str: str):
        """
        Retrieves 1-minute aggregated OHLC data for all US stocks (15-min delayed on Starter plan)

        Docs: https://polygon.io/docs/stocks/get_v2_aggs_grouped_locale_us_market_stocks__date
        """
        url = f"{BASE_URL}/v2/aggs/grouped/locale/us/market/stocks/{date_str}"
        params = {"adjusted": "true", "apiKey": self.api_key}

        r = requests.get(url, params=params)
        r.raise_for_status()
        return r.json()

    @backoff.on_exception(backoff.expo, Exception, max_tries=5)
    def get_agg(self, symbol: str, multiplier: int, timespan: str, start: str, end: str):
        """
        Generic aggregates request for historical and custom intervals.
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
        """
        Retrieves previous close values for a single ticker
        """
        url = f"{BASE_URL}/v2/aggs/ticker/{symbol}/prev"
        params = {"adjusted": "true", "apiKey": self.api_key}
        r = requests.get(url, params=params)
        r.raise_for_status()
        return r.json()
