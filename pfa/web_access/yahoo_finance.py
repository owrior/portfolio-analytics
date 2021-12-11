import datetime as dt

import pandas as pd
import yfinance as yf
from sqlalchemy.orm import Query

from pfa.models.date_config import DateConfig
from pfa.models.metric_config import MetricConfig
from pfa.readwrite import frame_to_sql, read_sql
from pfa.web_access.update_and_cache import get_most_recent_stock_dates


def populate_yahoo_stock_values():
    date_config, metric_config = _get_config_tables()
    stock_dates = get_most_recent_stock_dates()
    tickers = stock_dates["yahoo_ticker"].to_list()

    if len(tickers) == 0:
        return None

    raw_stock_values = _download_stock_values(stock_dates)

    if raw_stock_values is not None:
        stock_values = _process_stock_values(raw_stock_values)

        stock_values = (
            stock_values.merge(date_config, on="date", how="inner")
            .merge(metric_config, on="metric", how="inner")
            .loc[:, ["stock_id", "date_id", "metric_id", "value"]]
        )
        frame_to_sql(stock_values, "stock_values")


def _get_config_tables():  # pragma: no cover
    date_config = read_sql(Query(DateConfig))
    metric_config = read_sql(Query(MetricConfig))
    return date_config, metric_config


def _download_stock_values(
    stock_dates: pd.DataFrame,
) -> pd.DataFrame:  # pragma: no cover
    stock_values = []
    for _, row in stock_dates.iterrows():
        kwargs = (
            {"period": "max"}
            if row.date in (pd.NaT, None)
            else {"start": row.date + dt.timedelta(days=1)}
        )
        if row.yahoo_ticker is not None and kwargs["start"].date() < dt.date.today():
            stock_values.append(
                yf.download(tickers=row.yahoo_ticker, **kwargs).assign(
                    stock_id=row.stock_id
                )
            )
    return pd.concat(stock_values) if len(stock_values) > 0 else None


def _process_stock_values(stock_values: pd.DataFrame) -> pd.DataFrame:
    return (
        stock_values.reset_index()
        .melt(id_vars=["Date", "stock_id"], var_name="metric")
        .rename(
            columns={
                "Date": "date",
            }
        )
    )
