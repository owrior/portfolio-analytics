import re
from typing import List

import pandas as pd
import yfinance as yf
from sqlalchemy.orm import Query

from pfa.models.date_config import DateConfig
from pfa.models.metric_config import MetricConfig
from pfa.models.stock_config import StockConfig
from pfa.readwrite import frame_to_sql, read_sql


def populate_yahoo_stock_values():

    stock_config, date_config, metric_config = _get_config_tables()

    tickers = stock_config["yahoo_ticker"].to_list()

    stock_values = _download_and_shape_stock_values(tickers)

    stock_values = (
        stock_values.merge(date_config, on="date", how="inner")
        .merge(metric_config, on="metric", how="inner")
        .merge(stock_config, on="yahoo_ticker", how="inner")
        .loc[:, ["stock_id", "date_id", "metric_id", "value"]]
    )

    frame_to_sql(stock_values, "stock_values")


def _get_config_tables():
    stock_config = read_sql(
        (
            Query(StockConfig)
            .with_entities(StockConfig.stock_id, StockConfig.yahoo_ticker)
            .filter(StockConfig.yahoo_ticker != None)
        )
    )

    date_config = read_sql(Query(DateConfig))

    metric_config = read_sql(Query(MetricConfig))

    return stock_config, date_config, metric_config


def _download_and_shape_stock_values(tickers: List[str]):
    return (
        yf.download(tickers=tickers, period="max")
        .unstack()
        .reset_index()
        .dropna()
        .rename(
            columns={
                "level_0": "metric",
                "level_1": "yahoo_ticker",
                "Date": "date",
                0: "value",
            }
        )
    )
