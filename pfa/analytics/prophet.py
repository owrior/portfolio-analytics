import pandas as pd
from prophet import Prophet
from sqlalchemy.orm import Query
from prefect.utilities import logging

from pfa.id_cache import analytics_id_cache, metric_id_cache, date_id_cache
from pfa.models.config import DateConfig, StockConfig
from pfa.models.values import StockValues, AnalyticsValues
from pfa.readwrite import frame_to_sql, read_sql


logger = logging.get_logger(__file__)


def run_prophet_forcast_for_stocks() -> None:
    existing_forecasts = get_existing_forecasts(analytics_id_cache.prophet)

    stock_config = read_sql(Query(StockConfig))
    stock_config = stock_config[~stock_config["stock_id"].isin(existing_forecasts)]

    date_config = read_sql(Query(DateConfig))

    forecasts = []

    for _, row in stock_config.iterrows():
        stock_data = get_stock_data(row.stock_id)
        if not stock_data.empty:
            m = Prophet()
            m.fit(stock_data)
            future = m.make_future_dataframe(60, include_history=True)
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
