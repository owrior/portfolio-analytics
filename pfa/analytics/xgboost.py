from xgboost import XGBRegressor

from pfa.analytics.sklearn import validate_performance
from pfa.id_cache import analytics_id_cache


def validate_xgboost_performance(stock_data, date_config, stock_id):
    return validate_performance(
        XGBRegressor, stock_data, date_config, stock_id, analytics_id_cache.xgboost
    )
