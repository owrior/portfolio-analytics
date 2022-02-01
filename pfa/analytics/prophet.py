import datetime as dt
import os
from typing import Any

import numpy as np
import pandas as pd
from prophet import Prophet
from sqlalchemy.orm import Query
from tqdm import tqdm

from pfa.analytics.data_manipulation import create_time_windows
from pfa.id_cache import analytics_id_cache
from pfa.id_cache import date_id_cache
from pfa.id_cache import metric_id_cache
from pfa.models.config import DateConfig
from pfa.models.config import StockConfig
from pfa.models.values import AnalyticsValues
from pfa.models.values import StockValues
from pfa.readwrite import frame_to_sql
from pfa.readwrite import read_sql


def run_prophet_forcast_for_stocks() -> None:
    existing_forecasts = get_existing_forecasts(analytics_id_cache.prophet)

    stock_config = read_sql(Query(StockConfig))
    stock_config = stock_config[~stock_config["stock_id"].isin(existing_forecasts)]

    date_config = read_sql(Query(DateConfig))

    forecasts = []

    for _, row in stock_config.iterrows():
        stock_data = get_stock_data(row.stock_id)
        if not stock_data.empty:
            with suppress_stdout_stderr():
                m = Prophet()
                m.fit(stock_data)
                future = m.make_future_dataframe(60, include_history=False)
                forecasts.append(
                    m.predict(future)[["ds", "yhat", "yhat_lower", "yhat_upper"]]
                    .melt(id_vars="ds", var_name="metric")
                    .rename(columns={"ds": "date"})
                    .merge(date_config, on="date", how="left")
                    .assign(
                        forecast_date_id=date_id_cache.todays_id,
                        analytics_id=analytics_id_cache.prophet,
                        stock_id=row.stock_id,
                        metric_id=lambda x: x.metric.map(
                            {
                                "yhat": metric_id_cache.prediction,
                                "yhat_lower": metric_id_cache.prediction_lower,
                                "yhat_upper": metric_id_cache.prediction_upper,
                            }
                        ),
                    )
                    .drop(columns=["metric", "date"])
                )
    if forecasts:
        frame_to_sql(pd.concat(forecasts), "analytics_values")


def validate_prophet_performance(stock_data, date_config, stock_id) -> None:
    stock_data = fill_stock_data_to_time_horizon(stock_data, date_config)

    stock_data_shards = create_time_windows(
        stock_data, 30, int(min(len(stock_data) / 3, 90))
    )
    with suppress_stdout_stderr():
        models = [Prophet().fit(data_shard) for data_shard in stock_data_shards]
        futures = [m.make_future_dataframe(0, include_history=True) for m in models]
        prediction_shards = [
            model.predict(future)[["ds", "yhat"]]
            for model, future in zip(models, futures)
        ]
    validation_metrics = pd.concat(
        [
            generate_validation_metrics(stock_data_shard, prediction_shard)
            for stock_data_shard, prediction_shard in zip(
                stock_data_shards, prediction_shards
            )
        ]
    )
    return (
        validation_metrics.melt(id_vars="date", var_name="metric")
        .merge(
            metric_id_cache.dataframe[["metric", "metric_id"]], on="metric", how="inner"
        )
        .merge(date_config, on="date", how="inner")
        .loc[:, ["date_id", "metric_id", "value"]]
        .assign(
            analytics_id=analytics_id_cache.prophet,
            stock_id=stock_id,
            forcast_date_id=date_id_cache.todays_id,
        )
    )


def generate_validation_metrics(true_data, predicted_data):
    data = true_data.merge(predicted_data, on="ds", how="inner")
    y, yhat = data["y"].values, data["yhat"].values
    return pd.DataFrame(
        {
            "date": [data["ds"].max()],
            "Mean error": [np.mean(y - yhat)],
            "Mean (abs) error": [np.mean(np.abs(y - yhat))],
            "RSME": [np.sqrt(np.mean(np.square(y - yhat)))],
        }
    )


def loop_through_stock_values(func) -> Any:
    stock_config = read_sql(Query(StockConfig))
    date_config = read_sql(Query(DateConfig))

    function_results = []

    for _, row in tqdm(stock_config.iterrows(), total=len(stock_config)):
        stock_data = get_stock_data(row.stock_id)
        if not stock_data.empty:
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


def get_existing_forecasts(analytics_id: int) -> pd.DataFrame:
    return read_sql(
        Query(AnalyticsValues)
        .with_entities(
            AnalyticsValues.stock_id,
        )
        .where(AnalyticsValues.forecast_date_id == date_id_cache.todays_id)
        .where(AnalyticsValues.analytics_id == analytics_id)
        .distinct()
    )["stock_id"].to_list()


class suppress_stdout_stderr(object):
    """
    A context manager for doing a "deep suppression" of stdout and stderr in
    Python, i.e. will suppress all print, even if the print originates in a
    compiled C/Fortran sub-function.
       This will not suppress raised exceptions, since exceptions are printed
    to stderr just before a script exits, and after the context manager has
    exited (at least, I think that is why it lets exceptions through).

    """

    def __init__(self):
        # Open a pair of null files
        self.null_fds = [os.open(os.devnull, os.O_RDWR) for x in range(2)]
        # Save the actual stdout (1) and stderr (2) file descriptors.
        self.save_fds = (os.dup(1), os.dup(2))

    def __enter__(self):
        # Assign the null pointers to stdout and stderr.
        os.dup2(self.null_fds[0], 1)
        os.dup2(self.null_fds[1], 2)

    def __exit__(self, *_):
        # Re-assign the real stdout/stderr back to (1) and (2)
        os.dup2(self.save_fds[0], 1)
        os.dup2(self.save_fds[1], 2)
        # Close the null files
        os.close(self.null_fds[0])
        os.close(self.null_fds[1])
