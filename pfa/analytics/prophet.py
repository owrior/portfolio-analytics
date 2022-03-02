import datetime as dt
import os

import numpy as np
import pandas as pd
from prophet import Prophet

from pfa.analytics.data_manipulation import clear_previous_analytics
from pfa.analytics.data_manipulation import create_time_windows
from pfa.analytics.data_manipulation import get_training_parameters
from pfa.db_admin import extract_columns
from pfa.id_cache import analytics_id_cache
from pfa.id_cache import date_id_cache
from pfa.id_cache import metric_id_cache
from pfa.models.values import AnalyticsValues


def prophet_forecast(stock_data, date_config, stock_id) -> pd.DataFrame:
    clear_previous_analytics(stock_id, analytics_id_cache.prophet)
    training_period, forecast_length = 270, 90
    stock_data, training_end = get_training_parameters(stock_data, training_period)
    with suppress_stdout_stderr():
        m = Prophet()
        m.fit(stock_data)
        future = m.make_future_dataframe(forecast_length, include_history=True)
        forecast = (
            m.predict(future)[["ds", "yhat", "yhat_lower", "yhat_upper"]]
            .melt(id_vars="ds", var_name="metric")
            .rename(columns={"ds": "date"})
            .merge(date_config, on="date", how="left")
            .assign(
                forecast_date_id=date_id_cache.todays_id,
                analytics_id=analytics_id_cache.prophet,
                stock_id=stock_id,
                metric_id=lambda x: x.metric.map(
                    {
                        "yhat": metric_id_cache.adj_close,
                        "yhat_lower": metric_id_cache.prediction_lower,
                        "yhat_upper": metric_id_cache.prediction_upper,
                    }
                ),
            )
        )
    return forecast.loc[
        forecast["date"] >= training_end,
        extract_columns(AnalyticsValues),
    ]


def validate_prophet_performance(stock_data, date_config, stock_id) -> pd.DataFrame:
    clear_previous_analytics(stock_id, analytics_id_cache.prophet, validation=True)
    stock_data = stock_data.loc[
        stock_data["ds"].dt.date >= dt.date.today() - dt.timedelta(days=360)
    ].reset_index(drop=True)
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
        validation_metrics.melt(id_vars="date", var_name="metric_id")
        .merge(date_config, on="date", how="inner")
        .assign(
            forecast_date_id=date_id_cache.todays_id,
            analytics_id=analytics_id_cache.prophet,
            stock_id=stock_id,
            value=lambda x: np.round(x["value"], decimals=4),
        )
        .loc[:, extract_columns(AnalyticsValues)]
    )


def generate_validation_metrics(true_data, predicted_data):
    data = true_data.merge(predicted_data, on="ds", how="inner")
    y, yhat = data["y"].values, data["yhat"].values
    return pd.DataFrame(
        {
            "date": [data["ds"].max()],
            metric_id_cache.mean_abs_error: [np.mean(np.abs(y - yhat))],
            metric_id_cache.rmse: [np.sqrt(np.mean(np.square(y - yhat)))],
            metric_id_cache.rmsle: [
                np.sqrt(np.mean(np.square(np.log(y + 1) - np.log(yhat + 1))))
            ],
        }
    )


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
