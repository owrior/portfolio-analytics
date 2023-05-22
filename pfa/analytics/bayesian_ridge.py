import prefect
from sklearn.linear_model import BayesianRidge

from pfa.analytics.sklearn import forecast, validate_performance
from pfa.id_cache import analytics_id_cache
from pfa.readwrite import frame_to_sql

KWARGS = {}


@prefect.task
def baysian_ridge_forecast(stock_data, date_config, stock_id):
    """
    Executes sklearn forcast with baysian ridge model.
    """
    forecast_values = forecast(
        BayesianRidge,
        stock_data,
        date_config,
        stock_id,
        analytics_id_cache.bayesian_ridge,
        KWARGS,
    )
    frame_to_sql(forecast_values, "analytics_values")
    return None


@prefect.task
def validate_baysian_ridge_performance(stock_data, date_config, stock_id):
    """
    Executes sklearn validation for baysian ridge model.
    """
    validation_performance_ = validate_performance(
        BayesianRidge,
        stock_data,
        date_config,
        stock_id,
        analytics_id_cache.bayesian_ridge,
        KWARGS,
    )
    frame_to_sql(validation_performance_, "analytics_values")
    return None
