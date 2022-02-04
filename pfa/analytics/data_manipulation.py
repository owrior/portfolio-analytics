import datetime as dt
from typing import Any
from typing import List

import numpy as np
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.orm import Query
from tqdm import tqdm

from pfa.db import execute_query
from pfa.id_cache import metric_id_cache
from pfa.models.config import DateConfig
from pfa.models.config import StockConfig
from pfa.models.values import AnalyticsValues
from pfa.models.values import StockValues
from pfa.readwrite import read_sql


def create_time_windows(
    time_series_data: pd.DataFrame, window_distance: int, window_size: int
) -> List[pd.DataFrame]:
    """
    Parameters
    ----------
    time_series_data : pd.DataFrame
        Data in the form of a date column (named date).
    window_start_delay : int
        The number of days between the beginning of each window.
    window_size : int
        Size of time windows in days.
    """
    index_values = time_series_data.index.values
    index_list = [
        index_values[i : i + window_size]
        for i in np.arange(
            0,
            index_values.shape[0] - window_size + 1,
            step=window_distance,
        )
        + np.remainder(index_values.shape[0] - window_size, window_distance)
    ]
    return [time_series_data.iloc[index, :] for index in index_list]


def loop_through_stocks(func) -> Any:
    stock_config = read_sql(Query(StockConfig))
    date_config = read_sql(Query(DateConfig))

    function_results = []

    for _, row in tqdm(stock_config.iterrows(), total=len(stock_config)):
        stock_data = get_stock_data(row.stock_id)
        if not stock_data.empty:
            stock_data = fill_stock_data_to_time_horizon(stock_data, date_config)
            function_results.append(func(stock_data, date_config, row.stock_id))
    return pd.concat(function_results)


def fill_stock_data_to_time_horizon(
    stock_data: pd.DataFrame, date_config: pd.DataFrame
):
    return (
        date_config.loc[
            (date_config["date"] >= stock_data["ds"].min())
            & (date_config["date"] <= stock_data["ds"].max()),
            ["date"],
        ]
        .rename(columns={"date": "ds"})
        .merge(stock_data, on="ds", how="left")
        .ffill()
    )


def get_stock_data(stock_id: int) -> pd.DataFrame:
    return read_sql(
        Query(StockValues)
        .with_entities(
            DateConfig.date.label("ds"),
            StockValues.value.label("y"),
        )
        .join(DateConfig, StockValues.date_id == DateConfig.date_id)
        .where(StockValues.stock_id == stock_id)
        .where(StockValues.metric_id == metric_id_cache.adj_close)
        .where(DateConfig.date >= dt.date.today() - dt.timedelta(weeks=104))
    )


def clear_previous_analytics(stock_id, analytics_id, validation: bool = False):
    query = (
        sa.delete(AnalyticsValues)
        .where(AnalyticsValues.stock_id == stock_id)
        .where(AnalyticsValues.analytics_id == analytics_id)
    )
    if validation:
        query.where(AnalyticsValues.metric_id.in_(metric_id_cache.validation_metrics))
    else:
        query.where(
            AnalyticsValues.metric_id.notin_(metric_id_cache.validation_metrics)
        )
    execute_query(query)
