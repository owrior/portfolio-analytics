import prefect
from xgboost import XGBRegressor

from pfa.analytics.sklearn import forecast
from pfa.analytics.sklearn import validate_performance
from pfa.id_cache import analytics_id_cache
from pfa.readwrite import frame_to_sql

KWARGS = {"eta": 0.1}


@prefect.task
def xgboost_forecast(stock_data, date_config, stock_id):
    forecast_ = forecast(
        XGBRegressor,
        stock_data,
        date_config,
        stock_id,
        analytics_id_cache.xgboost,
        KWARGS,
    )
    frame_to_sql(forecast_, "analytics_values")
    return None


def validate_xgboost_performance(stock_data, date_config, stock_id):
    return validate_performance(
        XGBRegressor,
        stock_data,
        date_config,
        stock_id,
        analytics_id_cache.xgboost,
        KWARGS,
    )
