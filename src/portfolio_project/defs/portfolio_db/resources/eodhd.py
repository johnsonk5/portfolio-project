import os
from datetime import date

import pandas as pd
import requests
from dagster import Field, StringSource, resource

from portfolio_project.defs.portfolio_db.resources.env import load_local_env


@resource(
    config_schema={
        "exchange_code": Field(StringSource, is_required=False, default_value="US"),
    }
)
def eodhd_resource(context) -> "EODHDClient":
    """
    Dagster resource for the EODHD API.

    Environment:
    - EODHD_API_KEY: API key for EODHD.
    """
    load_local_env()
    api_key = os.getenv("EODHD_API_KEY")
    if not api_key:
        raise ValueError("EODHD_API_KEY environment variable must be set")

    exchange_code = str(context.resource_config.get("exchange_code") or "US").strip().upper()
    return EODHDClient(api_key=api_key, exchange_code=exchange_code)


class EODHDClient:
    """Client for interacting with the EODHD bulk end-of-day API."""

    def __init__(self, api_key: str, exchange_code: str = "US") -> None:
        self.api_key = api_key
        self.exchange_code = exchange_code
        self.base_url = "https://eodhd.com/api"
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "portfolio-project/0.1"})

    def get_bulk_eod_prices_df(self, trade_date: date) -> pd.DataFrame:
        """
        Fetch bulk end-of-day prices for the configured exchange on a specific trading date.
        """
        url = f"{self.base_url}/eod-bulk-last-day/{self.exchange_code}"
        response = self.session.get(
            url,
            params={
                "api_token": self.api_key,
                "date": trade_date.isoformat(),
                "fmt": "json",
            },
            timeout=60,
        )
        response.raise_for_status()

        payload = response.json()
        if isinstance(payload, list):
            return pd.DataFrame(payload)
        if isinstance(payload, dict):
            if "data" in payload and isinstance(payload["data"], list):
                return pd.DataFrame(payload["data"])
            if "error" in payload:
                raise ValueError(f"EODHD API error: {payload['error']}")
        return pd.DataFrame()
