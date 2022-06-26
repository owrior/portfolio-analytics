import prefect
from sklearn.linear_model import BayesianRidge

from pfa.analytics.sklearn import forecast
from pfa.analytics.sklearn import validate_performance
from pfa.id_cache import analytics_id_cache
from pfa.readwrite import frame_to_sql

KWARGS = {}


@prefect.task
def baysian_ridge_forecast(stock_data, date_config, stock_id):
    forecast_ = forecast(
        BayesianRidge,
        stock_data,
        date_config,
        stock_id,
        analytics_id_cache.bayesian_ridge,
        KWARGS,
    )
    frame_to_sql(forecast_, "analytics_values")
    return None


@prefect.task
def validate_baysian_ridge_performance(stock_data, date_config, stock_id):
    return validate_performance(
        BayesianRidge,
        stock_data,
        date_config,
        stock_id,
        analytics_id_cache.bayesian_ridge,
        KWARGS,
    )
