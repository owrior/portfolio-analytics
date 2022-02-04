from xgboost import XGBRegressor

from pfa.analytics.sklearn import forecast
from pfa.analytics.sklearn import validate_performance
from pfa.id_cache import analytics_id_cache


def xgboost_forecast(stock_data, date_config, stock_id):
    kwargs = {"eta": 0.1}
    return forecast(
        XGBRegressor,
        stock_data,
        date_config,
        stock_id,
        analytics_id_cache.xgboost,
        kwargs,
    )


def validate_xgboost_performance(stock_data, date_config, stock_id):
    kwargs = {"eta": 0.1}
    return validate_performance(
        XGBRegressor,
        stock_data,
        date_config,
        stock_id,
        analytics_id_cache.xgboost,
        kwargs,
    )
