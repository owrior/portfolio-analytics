import datetime as dt
from typing import Tuple

import numpy as np
import pandas as pd
from sklearn.model_selection import cross_validate

from pfa.analytics.data_manipulation import clear_previous_analytics
from pfa.analytics.data_manipulation import create_time_windows
from pfa.id_cache import analytics_id_cache
from pfa.id_cache import date_id_cache
from pfa.id_cache import metric_id_cache


def forecast(Model, stock_data, date_config, stock_id, analytics_id, kwargs):
    clear_previous_analytics(stock_id, analytics_id_cache.xgboost)
    training_period, forecast_length = 270, 90
    training_start = dt.date.today() - dt.timedelta(days=training_period + 1)
    stock_data = stock_data.loc[
        stock_data["ds"].dt.date > training_start,
        :,
    ].copy()
    training_end = stock_data["ds"].max()
    stock_data["adj_close"] = stock_data["y"]
    stock_data = transform_prediction_and_create_x(stock_data)

    if len(stock_data) < training_period:
        training_period = len(stock_data)

    stock_data = create_features(stock_data)
    x, y = get_xy(stock_data, training_period)
    m = Model(**kwargs).fit(x, y)
    stock_data = (
        pd.DataFrame(
            {
                "ds": pd.date_range(
                    stock_data["ds"].min(),
                    stock_data["ds"].max() + dt.timedelta(days=forecast_length),
                )
            }
        )
        .merge(stock_data, on="ds", how="left")
        .rename(columns={"ds": "date"})
        .merge(date_config, on="date", how="inner")
    )
    date_id_zero = int(stock_data["date_id"].min())

    for days_forward in np.arange(0, forecast_length):
        x_pred, y_pred = get_xy(
            stock_data.iloc[: (training_period + days_forward), :],
            training_period + days_forward,
        )
        y_hat = m.predict(x_pred)
        stock_data["y_hat"] = np.concatenate(
            (
                y_hat,
                np.where(
                    np.zeros(shape=(forecast_length - days_forward,)) == 0,
                    np.nan,
                    0,
                ),
            ),
        )
        t_plus_one = date_id_zero + (training_period + days_forward)
        if days_forward == 0:
            stock_data["x"] = np.where(
                stock_data["date_id"] == t_plus_one,
                stock_data["y"].shift(),
                stock_data["x"],
            )
            stock_data["adj_close"] = np.where(
                stock_data["date_id"] == t_plus_one,
                np.power(np.e, stock_data["x"]) * stock_data["adj_close"].shift(),
                stock_data["adj_close"],
            )
        else:
            stock_data["x"] = np.where(
                stock_data["date_id"] == t_plus_one,
                stock_data["y_hat"].shift(),
                stock_data["x"],
            )
            stock_data["adj_close"] = np.where(
                stock_data["date_id"] == t_plus_one,
                np.power(np.e, stock_data["x"]) * stock_data["adj_close"].shift(),
                stock_data["adj_close"],
            )
        stock_data = create_features(stock_data)

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
        stock_data["date"] >= training_end,
        [
            "forecast_date_id",
            "analytics_id",
            "stock_id",
            "metric_id",
            "date_id",
            "value",
        ],
    ]


def validate_performance(
    Model, stock_data, date_config, stock_id, analytics_id, kwargs
):
    score_mapping = {
        "neg_mean_absolute_error": metric_id_cache.mean_abs_error,
        "neg_mean_squared_error": metric_id_cache.rmse,
    }
    clear_previous_analytics(stock_id, analytics_id_cache.xgboost, validation=True)

    stock_data = transform_prediction_and_create_x(stock_data)
    stock_data = create_features(stock_data)
    window_size = int(min(len(stock_data) / 3, 90))
    stock_data_shards = create_time_windows(stock_data, 30, window_size)

    scores = []
    for shard in stock_data_shards:
        x, y = get_xy(shard, window_size)
        model = Model(**kwargs).fit(x, y)
        scores.append(
            pd.DataFrame(
                cross_validate(model, x, y, scoring=[*score_mapping.keys()], cv=5)
            )
            .mean()
            .round()
            .reset_index()
            .rename(columns={0: "value"})
            .assign(
                analytics_id=analytics_id,
                stock_id=stock_id,
                date=shard["ds"].max(),
                metric_id=lambda x: x["index"].map(
                    {"test_" + key: val for key, val in score_mapping.items()}
                ),
                value=lambda x: np.absolute(
                    np.round(x["value"], decimals=4),
                ),
            )
            .drop(columns="index")
            .dropna()
        )

    return (
        pd.concat(scores)
        .merge(date_config, on="date", how="inner")
        .drop(columns="date")
        .assign(forecast_date_id=date_id_cache.todays_id)
    )


def transform_prediction_and_create_x(stock_data: pd.DataFrame) -> pd.DataFrame:
    stock_data["y"] = np.log(stock_data["y"] / stock_data["y"].shift())
    stock_data = stock_data.dropna().copy()
    stock_data["x"] = stock_data["y"].shift().bfill()
    return stock_data


def create_features(stock_data: pd.DataFrame) -> pd.DataFrame:
    stock_data["x_std"] = stock_data["x"].std()
    stock_data["x_bar_7d"] = stock_data["x"].rolling(7, min_periods=4).mean().bfill()
    stock_data["x_bar_28d"] = stock_data["x"].rolling(28, min_periods=14).mean().bfill()
    return stock_data


def get_xy(df: pd.DataFrame, window_size: int) -> Tuple[np.array]:
    return (
        df.filter(like="x").values.reshape(
            (window_size, len(df.filter(like="x").columns))
        ),
        df["y"].values,
    )
