import datetime as dt
from typing import Tuple

import numpy as np
import pandas as pd

from pfa.analytics.calculated_metrics import rmse
from pfa.analytics.calculated_metrics import rmsle
from pfa.analytics.calculated_metrics import smape
from pfa.analytics.data_manipulation import clear_previous_analytics
from pfa.analytics.data_manipulation import get_cutoffs
from pfa.analytics.data_manipulation import get_training_parameters
from pfa.analytics.data_manipulation import unscale_natural_log
from pfa.db_admin import extract_columns
from pfa.id_cache import date_id_cache
from pfa.id_cache import metric_id_cache
from pfa.models.values import AnalyticsValues


def forecast(Model, stock_data, date_config, stock_id, analytics_id, kwargs):
    """
    Train an sklearn structured model and forecast stock data.
    """
    clear_previous_analytics(stock_id, analytics_id)
    stock_data = stock_data.copy()
    training_period, forecast_length = 270, 90
    stock_data, training_end = get_training_parameters(stock_data, training_period)

    stock_data["adj_close"] = stock_data["y"]
    stock_data = transform_prediction_and_create_x(stock_data)

    if len(stock_data) < training_period:
        training_period = len(stock_data)

    stock_data = stock_data.loc[
        stock_data["ds"].dt.date >= dt.date.today() - dt.timedelta(days=360)
    ].reset_index(drop=True)

    stock_data = create_features(stock_data)

    stock_data = predict_forward(
        Model, stock_data, date_config, training_period, forecast_length, kwargs
    )

    stock_data = stock_data.assign(
        forecast_date_id=date_id_cache.todays_id,
        analytics_id=analytics_id,
        stock_id=stock_id,
    )
    stock_data = pd.concat(
        [
            stock_data.rename(columns={"x": "value"}).assign(
                metric_id=metric_id_cache.log_return
            ),
            stock_data.rename(columns={"adj_close": "value"}).assign(
                metric_id=metric_id_cache.adj_close
            ),
        ]
    )
    return stock_data.loc[
        stock_data["date"] >= training_end, extract_columns(AnalyticsValues)
    ]


def validate_performance(
    Model, stock_data, date_config, stock_id, analytics_id, kwargs
):
    """
    Perform cross validation for model for anm sklearn style model.
    """
    stock_data = stock_data.copy()
    clear_previous_analytics(stock_id, analytics_id, validation=True)

    stock_data["adj_close"] = stock_data["y"]
    stock_data = transform_prediction_and_create_x(stock_data)
    stock_data = create_features(stock_data)

    stock_data = stock_data.loc[
        stock_data["ds"].dt.date >= dt.date.today() - dt.timedelta(days=360)
    ].reset_index(drop=True)

    initial = 180
    step_size = 30
    horizon = 30
    cutoffs = get_cutoffs(stock_data["ds"], initial, step_size)
    validation_scores = []
    for cutoff in cutoffs:
        initial_period = stock_data.loc[stock_data["ds"] < cutoff, :]
        x, y = get_xy(initial_period, len(initial_period))
        m = Model(**kwargs).fit(x, y)
        training_period = stock_data[stock_data["ds"] == cutoff].index[0]
        predicted_data = predict_forward(
            m,
            initial_period,
            date_config,
            training_period,
            horizon,
            kwargs,
            trained=True,
        )
        comparisons = (
            predicted_data.loc[
                predicted_data["date"] >= cutoff, ["date", "y_hat", "adj_close"]
            ]
            .rename(columns={"adj_close": "adj_close_hat"})
            .merge(
                stock_data.rename(columns={"ds": "date"}).loc[
                    :, ["date", "y", "adj_close"]
                ],
                on="date",
                how="inner",
            )
        )
        validation_scores.append(
            pd.DataFrame(
                {
                    "metric_id": [
                        metric_id_cache.rmse,
                        metric_id_cache.rmsle,
                        metric_id_cache.smape,
                    ],
                    "value": [
                        rmse(comparisons["adj_close"], comparisons["adj_close_hat"]),
                        rmsle(comparisons["adj_close"], comparisons["adj_close_hat"]),
                        smape(comparisons["adj_close"], comparisons["adj_close_hat"]),
                    ],
                }
            ).assign(
                analytics_id=analytics_id,
                stock_id=stock_id,
                date=cutoff + dt.timedelta(days=horizon),
            )
        )

    return (
        pd.concat(validation_scores)
        .merge(date_config, on="date", how="inner")
        .assign(forecast_date_id=date_id_cache.todays_id)
        .loc[:, extract_columns(AnalyticsValues)]
    )


def predict_forward(
    Model,
    stock_data,
    date_config,
    training_period,
    forecast_length,
    kwargs,
    trained=False,
):
    """
    Performs forecasting up to a certain time into the future.

    Sets the previous known values as the initial conditions (forward filling)
    then iterates until the maximum change between iterations is below the tol-
    erance.

    Then calculates the adjusted close based on the predicted log change.
    """
    if trained:
        m = Model
    else:
        x, y = get_xy(stock_data, training_period)
        m = Model(**kwargs).fit(x, y)
    stock_data = (
        pd.DataFrame(
            {
                "ds": pd.date_range(
                    stock_data["ds"].min(),
                    stock_data["ds"].max() + dt.timedelta(days=int(forecast_length)),
                )
            }
        )
        .merge(stock_data, on="ds", how="left")
        .rename(columns={"ds": "date"})
        .merge(date_config, on="date", how="inner")
        .ffill()
        .assign(y_hat=lambda x: x.y)
    )
    stock_data.loc[training_period:, "x"] = stock_data["y_hat"].iloc[-1]

    tolerance = 1e-9
    for _ in range(10000):
        # Get features
        x_pred, y_pred = get_xy(stock_data.tail(forecast_length), forecast_length)

        # Generate forecast
        y_hat = m.predict(x_pred)

        # Assign forecast values
        stock_data.loc[training_period:, "y_hat"] = y_hat

        # Assign unknown initial value
        stock_data.loc[(training_period + 1) :, "x"] = y_hat[:-1]

        # Recreate features
        stock_data = stock_data.pipe(create_features)

        # Initialise the check variable forcing it to have a non-zero absolute
        # difference for the first iteration.
        if _ == 0:
            y_hat_minus_1 = -y_hat

        # Check condition
        if np.max(np.abs(y_hat - y_hat_minus_1)) < tolerance:
            break
        y_hat_minus_1 = y_hat

    # Calculate adjusted close for forecast horizon
    initial_adj_close = stock_data["adj_close"].iloc[-1]
    adj_close = np.zeros(forecast_length)
    for _, increase in enumerate(unscale_natural_log(y_hat)):
        if _ == 0:
            # Need to start with initial value when the previous will be before
            # the forecast range.
            adj_close[_] = initial_adj_close * increase
        else:
            # Else calculate based on the previously calculated value.
            adj_close[_] = adj_close[_ - 1] * increase
    stock_data.loc[training_period:, "adj_close"] = adj_close
    return stock_data


def transform_prediction_and_create_x(stock_data: pd.DataFrame) -> pd.DataFrame:
    """
    Transform adjusted close to log difference and create x as y t-1.
    """
    stock_data["y"] = np.log(stock_data["y"] / stock_data["y"].shift())
    stock_data = stock_data.dropna().copy()
    stock_data["x"] = stock_data["y"].shift().bfill()
    return stock_data


def create_features(stock_data: pd.DataFrame) -> pd.DataFrame:
    """
    Create features to be used in modelling.
    """
    stock_data["x_std"] = stock_data["x"].std()
    stock_data["x_bar_7d"] = stock_data["x"].rolling(7, min_periods=4).mean().bfill()
    stock_data["x_bar_28d"] = stock_data["x"].rolling(28, min_periods=14).mean().bfill()
    return stock_data


def get_xy(df: pd.DataFrame, window_size: int) -> Tuple[np.array]:
    """
    Split dataframe into x and y numpy arrays.
    """
    return (
        df.filter(like="x").values.reshape(
            (window_size, len(df.filter(like="x").columns))
        ),
        df["y"].values,
    )
