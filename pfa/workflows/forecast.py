from prefect import Flow, unmapped

from pfa.analytics.bayesian_ridge import baysian_ridge_forecast
from pfa.analytics.data_manipulation import (
    get_date_config,
    get_stock_data_to_forecast,
    get_stock_ids,
)
from pfa.analytics.prophet import prophet_forecast
from pfa.analytics.xgboost import xgboost_forecast

with Flow("Forecast") as flow:
    date_config = get_date_config()
    stock_ids = get_stock_ids()
    stock_data = get_stock_data_to_forecast(stock_ids, date_config)

    prophet_forcasted = prophet_forecast.map(
        stock_data=stock_data, date_config=unmapped(date_config), stock_id=stock_ids
    )

    xgboost_forecasted = xgboost_forecast.map(
        stock_data=stock_data, date_config=unmapped(date_config), stock_id=stock_ids
    )

    baysian_ridge_forecasted = baysian_ridge_forecast.map(
        stock_data=stock_data, date_config=unmapped(date_config), stock_id=stock_ids
    )
