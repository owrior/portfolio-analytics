import prefect
from xgboost import XGBRegressor

from pfa.analytics.sklearn import forecast
from pfa.analytics.sklearn import validate_performance
from pfa.id_cache import analytics_id_cache
from pfa.readwrite import frame_to_sql

KWARGS = {"eta": 0.1}


@prefect.task
def xgboost_forecast(stock_data, date_config, stock_id):
    """
    Executes sklearn forcast with xgboost model.
    """
    forecast_values = forecast(
        XGBRegressor,
        stock_data,
        date_config,
        stock_id,
        analytics_id_cache.xgboost,
        KWARGS,
    )
    frame_to_sql(forecast_values, "analytics_values")
    return None


@prefect.task
def validate_xgboost_performance(stock_data, date_config, stock_id):
    """
    Executes sklearn validation for xgboost model.
    """
    validation_performance = validate_performance(
        XGBRegressor,
        stock_data,
        date_config,
        stock_id,
        analytics_id_cache.xgboost,
        KWARGS,
    )
    frame_to_sql(validation_performance, "analytics_values")
    return None
