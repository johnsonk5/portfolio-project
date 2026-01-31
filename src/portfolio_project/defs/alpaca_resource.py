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
from dagster import resource


@resource
def alpaca_resource(context) -> "AlpacaClient":
    """
    Dagster resource for Alpaca Markets API client.
    
    Configuration:
    - ALPACA_API_KEY: API key for Alpaca (from environment)
    - ALPACA_SECRET_KEY: Secret key for Alpaca (from environment)
    """
    api_key = os.getenv("ALPACA_API_KEY")
    secret_key = os.getenv("ALPACA_SECRET_KEY")
    
    if not api_key or not secret_key:
        raise ValueError("ALPACA_API_KEY and ALPACA_SECRET_KEY environment variables must be set")
    
    return AlpacaClient(api_key, secret_key)


class AlpacaClient:
    """Client for interacting with the Alpaca Markets API."""
    
    def __init__(self, api_key: str, secret_key: str):
        self.api_key = api_key
        self.secret_key = secret_key
        self.client = StockHistoricalDataClient(api_key, secret_key)
        self.trading_client = TradingClient(api_key, secret_key)
    
    def get_bars(
        self,
        symbol: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> dict:
        """
        Fetch 5-minute bar (OHLCV) data for a given symbol.
        
        Args:
            symbol: Stock ticker symbol (e.g., "AAPL")
            start_date: Start date for data retrieval (defaults to last business day)
            end_date: End date for data retrieval (defaults to today)
        
        Returns:
            Dictionary with symbol as key and DataFrame of bar data as value
        """
        # Set default dates if not provided
        if end_date is None:
            end_date = datetime.now(timezone.utc)
        if start_date is None:
            # Default to last 7 days
            start_date = end_date - timedelta(days=7)
        
        # 5-minute timeframe
        tf = TimeFrame(5, TimeFrameUnit.Minute)
        
        request = StockBarsRequest(
            symbol_or_symbols=[symbol],
            timeframe=tf,
            start=start_date,
            end=end_date,
        )
        
        bars = self.client.get_stock_bars(request)
        
        return bars.df.to_dict() if hasattr(bars, 'df') else bars

    def get_bars_df(
        self,
        symbol: str,
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

        tf = TimeFrame(5, TimeFrameUnit.Minute)

        request = StockBarsRequest(
            symbol_or_symbols=[symbol],
            timeframe=tf,
            start=start_date,
            end=end_date,
        )

        bars = self.client.get_stock_bars(request)

        if hasattr(bars, "df"):
            return bars.df.copy()
        return pd.DataFrame(bars)

    def get_assets(
        self,
        status: AssetStatus = AssetStatus.ACTIVE,
        asset_class: AssetClass = AssetClass.US_EQUITY,
    ) -> list:
        """
        Fetch the list of tradable assets (tickers) from Alpaca.
        """
        request = GetAssetsRequest(status=status, asset_class=asset_class)
        return self.trading_client.get_all_assets(request)

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
