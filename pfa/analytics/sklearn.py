from sklearn.model_selection import cross_validate
import pandas as pd
import numpy as np
from prefect.utilities import logging
import sqlalchemy as sa


from pfa.analytics.data_manipulation import create_time_windows
from pfa.id_cache import metric_id_cache
from pfa.id_cache import date_id_cache
from pfa.db import get_engine
from pfa.models.values import AnalyticsValues


logger = logging.get_logger(__file__)


def validate_performance(Model, stock_data, date_config, stock_id, analytics_id):
    score_mapping = {
        "neg_mean_absolute_error": metric_id_cache.mean_abs_error,
        "neg_mean_squared_error": metric_id_cache.rmse,
    }
    # Clear the existing metrics and analytics
    with get_engine().begin() as conn:
        conn.execute(
            sa.delete(AnalyticsValues)
            .where(AnalyticsValues.stock_id == stock_id)
            .where(AnalyticsValues.analytics_id == analytics_id)
            .where(AnalyticsValues.metric_id.in_([*score_mapping.values()]))
        )

    stock_data["x"] = stock_data["y"].shift().bfill()
    stock_data["x_bar_we"] = stock_data["y"].shift().rolling(7).mean().bfill()
    stock_data["x_bar_mo"] = stock_data["y"].shift().rolling(28).mean().bfill()
    window_size = int(min(len(stock_data) / 3, 90))
    stock_data_shards = create_time_windows(stock_data, 30, window_size)

    scores = []
    for shard in stock_data_shards:
        x, y = (
            shard.filter(like="x").values.reshape((window_size, 3)),
            shard["y"].values,
        )
        model = Model(eta=0.1).fit(x, y)
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
