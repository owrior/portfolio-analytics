import datetime as dt
import os

import numpy as np
import pandas as pd
import prefect
from prophet import Prophet
from prophet.diagnostics import cross_validation

from pfa.analytics.calculated_metrics import get_metric_function_mapping
from pfa.analytics.data_manipulation import clear_previous_analytics
from pfa.analytics.data_manipulation import get_cutoffs
from pfa.analytics.data_manipulation import get_training_parameters
from pfa.db_admin import extract_columns
from pfa.id_cache import analytics_id_cache
from pfa.id_cache import date_id_cache
from pfa.id_cache import metric_id_cache
from pfa.models.values import AnalyticsValues
from pfa.readwrite import frame_to_sql


def get_prophet_model():
    """
    Get the prophet model to run.
    """
    return Prophet(daily_seasonality=False, yearly_seasonality=False).add_seasonality(
        name="quarterly", period=365.25 / 4, fourier_order=5, prior_scale=15
    )


@prefect.task
def prophet_forecast(stock_data, date_config, stock_id) -> pd.DataFrame:
    clear_previous_analytics(stock_id, analytics_id_cache.prophet)
    stock_data = stock_data.copy()
    training_period, forecast_length = 270, 90
    stock_data, training_end = get_training_parameters(stock_data, training_period)
    with suppress_stdout_stderr():
        m = get_prophet_model()
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
    forecast_ = forecast.loc[
        forecast["date"] >= training_end,
        extract_columns(AnalyticsValues),
    ]
    frame_to_sql(forecast_, "analytics_values")
    return None


@prefect.task
def validate_prophet_performance(stock_data, date_config, stock_id) -> pd.DataFrame:
    clear_previous_analytics(stock_id, analytics_id_cache.prophet, validation=True)
    stock_data = stock_data.copy()
    stock_data = stock_data.loc[
        stock_data["ds"].dt.date >= dt.date.today() - dt.timedelta(days=360)
    ].reset_index(drop=True)
    cutoffs = get_cutoffs(stock_data["ds"], 180, 30)

    with suppress_stdout_stderr():
        m = get_prophet_model().fit(stock_data)
        cv = cross_validation(m, cutoffs=cutoffs, horizon="30 days")
        validation_metrics = [
            cv.groupby("cutoff")["y", "yhat"]
            .apply(lambda x: function(x["y"], x["yhat"]))
            .reset_index()
            .rename(columns={"cutoff": "date", 0: "value"})
            .assign(
                metric_id=metric_id,
                date=lambda x: x["date"] + dt.timedelta(days=30),
            )
            for metric_id, function in get_metric_function_mapping()
        ]
    return (
        pd.concat(validation_metrics)
        .merge(date_config, on="date", how="inner")
        .assign(
            forecast_date_id=date_id_cache.todays_id,
            analytics_id=analytics_id_cache.prophet,
            stock_id=stock_id,
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
        self.null_fds = [os.open(os.devnull, os.O_RDWR) for _ in range(2)]
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
