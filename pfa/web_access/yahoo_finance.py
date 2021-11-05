import re
import pandas as pd
from sqlalchemy.orm import Query
import yfinance as yf
from typing import List

from pfa.models.date import Date
from pfa.models.stock_config import StockConfig
from pfa.models.metric import Metric

from pfa.readwrite import read_sql
from pfa.readwrite import frame_to_sql


def populate_stock_values():

    stock_config, date, metric = _get_config_tables()

    tickers = stock_config["yahoo_ticker"].to_list()

    stock_values = _download_and_shape_stock_values(tickers)

    stock_values = (
        stock_values.merge(date, on="date", how="inner")
        .merge(metric, on="metric", how="inner")
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

    date = read_sql(Query(Date))

    metric = read_sql(Query(Metric))

    return stock_config, date, metric


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
