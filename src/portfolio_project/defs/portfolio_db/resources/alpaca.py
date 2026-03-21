import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import AssetClass, AssetStatus
from alpaca.trading.requests import GetAssetsRequest
from dagster import Bool, Field, resource

from portfolio_project.defs.portfolio_db.resources.env import load_local_env


@resource(
    config_schema={
        "paper": Field(Bool, is_required=False),
    }
)
def alpaca_resource(context) -> "AlpacaClient":
    """
    Dagster resource for Alpaca Markets API client.

    Configuration (resource config):
    - paper: Whether to use Alpaca paper trading endpoints for trading API calls.

    Environment:
    - ALPACA_API_KEY: API key for Alpaca (from environment)
    - ALPACA_SECRET_KEY: Secret key for Alpaca (from environment)
    - ALPACA_PAPER: Optional boolean override when `paper` is not provided.
    """
    load_local_env()
    api_key = os.getenv("ALPACA_API_KEY")
    secret_key = os.getenv("ALPACA_SECRET_KEY")

    if not api_key or not secret_key:
        raise ValueError("ALPACA_API_KEY and ALPACA_SECRET_KEY environment variables must be set")

    ## In future releases, it will be possible to paper or live trade through Alpaca.
    ## As such, this option allows you to choose paper or live trading.

    env_paper = os.getenv("ALPACA_PAPER")
    if env_paper is not None:
        env_paper = env_paper.strip().lower() in {"1", "true", "t", "yes", "y", "on"}
    config_paper = context.resource_config.get("paper")
    if config_paper is not None:
        paper = config_paper
    elif env_paper is not None:
        paper = env_paper
    else:
        paper = True

    return AlpacaClient(api_key, secret_key, paper=paper)


class AlpacaClient:
    """Client for interacting with the Alpaca Markets API."""

    def __init__(self, api_key: str, secret_key: str, paper: bool = True):
        self.api_key = api_key
        self.secret_key = secret_key
        self.paper = paper
        self.client = StockHistoricalDataClient(api_key, secret_key)
        self.trading_client = TradingClient(api_key, secret_key, paper=paper)

    ## Pull 5-minute bars; switch the timeframe here if needed.
    def get_bars_df(
        self,
        symbol_or_symbols,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """
        Fetch 5-minute bar (OHLCV) data for a given symbol as a DataFrame.
        """
        if end_date is None:
            end_date = datetime.now(timezone.utc)
        if start_date is None:
            start_date = end_date - timedelta(days=7)

        # 5-minute timeframe - edit this to choose a different time frame
        # Some sample time frames are included below.
        tf = TimeFrame(5, TimeFrameUnit.Minute)

        # tf = TimeFrame(15, TimeFrameUnit.Minute) -- 15 minute bars

        # tf = TimeFrame(1, TimeFrameUnit.Hour) -- 1 hour bars

        if isinstance(symbol_or_symbols, str):
            symbols = [symbol_or_symbols]
        else:
            symbols = list(symbol_or_symbols)

        request = StockBarsRequest(
            symbol_or_symbols=symbols,
            timeframe=tf,
            start=start_date,
            end=end_date,
        )

        bars = self.client.get_stock_bars(request)

        if hasattr(bars, "df"):
            return bars.df.copy()
        return pd.DataFrame(bars)

    def get_daily_bars_df(
        self,
        symbol_or_symbols,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """
        Fetch daily OHLCV data for one or more symbols as a DataFrame.
        """
        if end_date is None:
            end_date = datetime.now(timezone.utc)
        if start_date is None:
            start_date = end_date - timedelta(days=30)

        tf = TimeFrame(1, TimeFrameUnit.Day)

        if isinstance(symbol_or_symbols, str):
            symbols = [symbol_or_symbols]
        else:
            symbols = list(symbol_or_symbols)

        request = StockBarsRequest(
            symbol_or_symbols=symbols,
            timeframe=tf,
            start=start_date,
            end=end_date,
        )

        bars = self.client.get_stock_bars(request)

        if hasattr(bars, "df"):
            return bars.df.copy()
        return pd.DataFrame(bars)

    def get_assets_df(
        self,
        status: AssetStatus = AssetStatus.ACTIVE,
        asset_class: AssetClass = AssetClass.US_EQUITY,
    ) -> pd.DataFrame:
        """
        Fetch the list of tradable assets as a DataFrame.
        """
        assets = self.get_assets(status=status, asset_class=asset_class)
        rows = []
        for asset in assets:
            if hasattr(asset, "model_dump"):
                rows.append(asset.model_dump())
            elif hasattr(asset, "dict"):
                rows.append(asset.dict())
            else:
                rows.append(asset.__dict__)
        return pd.DataFrame(rows)

    def get_assets(
        self,
        status: AssetStatus = AssetStatus.ACTIVE,
        asset_class: AssetClass = AssetClass.US_EQUITY,
    ):
        request = GetAssetsRequest(status=status, asset_class=asset_class)
        return self.trading_client.get_all_assets(request)
