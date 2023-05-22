from prefect import Flow, unmapped

from pfa.analytics.bayesian_ridge import validate_baysian_ridge_performance
from pfa.analytics.data_manipulation import (
    get_date_config,
    get_stock_data_to_forecast,
    get_stock_ids,
)
from pfa.analytics.prophet import validate_prophet_performance
from pfa.analytics.xgboost import validate_xgboost_performance

with Flow("Validation") as flow:
    date_config = get_date_config()
    stock_ids = get_stock_ids()
    stock_data = get_stock_data_to_forecast(stock_ids, date_config)

    prophet_validated = validate_prophet_performance.map(
        stock_data=stock_data, date_config=unmapped(date_config), stock_id=stock_ids
    )

    xgboost_validated = validate_xgboost_performance.map(
        stock_data=stock_data, date_config=unmapped(date_config), stock_id=stock_ids
    )

    baysian_ridge_validated = validate_baysian_ridge_performance.map(
        stock_data=stock_data, date_config=unmapped(date_config), stock_id=stock_ids
    )
