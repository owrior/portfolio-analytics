from xgboost import XGBRegressor

from pfa.analytics.sklearn import forecast
from pfa.analytics.sklearn import validate_performance
from pfa.id_cache import analytics_id_cache

KWARGS = {"eta": 0.1}


def xgboost_forecast(stock_data, date_config, stock_id):
    return forecast(
        XGBRegressor,
        stock_data,
        date_config,
        stock_id,
        analytics_id_cache.xgboost,
        KWARGS,
    )


def validate_xgboost_performance(stock_data, date_config, stock_id):
    return validate_performance(
        XGBRegressor,
        stock_data,
        date_config,
        stock_id,
        analytics_id_cache.xgboost,
        KWARGS,
    )
