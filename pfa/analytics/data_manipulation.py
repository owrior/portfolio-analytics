import datetime as dt
from typing import List
from typing import Tuple

import numpy as np
import pandas as pd
import prefect
import sqlalchemy as sa
from sqlalchemy.orm import Query

from pfa.db import execute_query
from pfa.id_cache import metric_id_cache
from pfa.models.config import DateConfig
from pfa.models.values import AnalyticsValues
from pfa.models.values import StockValues
from pfa.readwrite import read_sql


def create_time_windows(
    time_series_data: pd.DataFrame, window_distance: int, window_size: int
) -> List[pd.DataFrame]:
    """
    Creates rolling time windows for analysis.

    Parameters
    ----------
    time_series_data : pd.DataFrame
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
    return [time_series_data.iloc[index - index_values[0], :] for index in index_list]


def get_cutoffs(dates: pd.Series, initial: int = None, step_size: int = None):
    """
    Get date cutoffs for cross validation.
    """
    total_period = len(dates)
    if initial is None:
        initial = total_period // 2
    if step_size is None:
        step_size = total_period // 12
    total_horizons = int(np.ceil((total_period - initial) / step_size))
    cutoffs = [dates[initial + (n * step_size) - 1] for n in range(total_horizons)]
    return [
        cutoff
        for cutoff in cutoffs
        if (dates.iloc[-1] - cutoff) >= dt.timedelta(days=30)
    ]


@prefect.task
def get_stock_ids() -> List[int]:
    """
    Fetches stock ids.
    """
    return read_sql(Query(StockValues).with_entities(StockValues.stock_id).distinct())[
        "stock_id"
    ].to_list()


@prefect.task
def get_date_config() -> pd.DataFrame:
    """
    Return date_config tables.
    """
    return read_sql(Query(DateConfig))


def get_stock_data(stock_id: int) -> pd.DataFrame:
    """
    Retrieves data for a particular stock.
    """
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
    """
    Clears database of a specific analytic.
    """
    query = (
        sa.delete(AnalyticsValues)
        .where(AnalyticsValues.stock_id == stock_id)
        .where(AnalyticsValues.analytics_id == analytics_id)
    )
    if validation:
        query = query.where(
            AnalyticsValues.metric_id.in_(metric_id_cache.validation_metrics)
        )
    else:
        query = query.where(
            AnalyticsValues.metric_id.notin_(metric_id_cache.validation_metrics)
        )
    execute_query(query)


def get_training_parameters(
    stock_data: pd.DataFrame, training_period: int
) -> Tuple[pd.DataFrame, dt.date]:
    training_start = dt.date.today() - dt.timedelta(days=training_period + 1)
    stock_data = stock_data.loc[
        stock_data["ds"].dt.date > training_start,
        :,
    ].copy()
    training_end = stock_data["ds"].max()
    return stock_data, training_end


def unscale_natural_log(x):
    return np.power(np.e, x)
