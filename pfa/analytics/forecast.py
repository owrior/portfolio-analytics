import pandas as pd
from prefect.utilities import logging

from pfa.analytics.bayesian_ridge import baysian_ridge_forecast
from pfa.analytics.data_manipulation import loop_through_stocks
from pfa.analytics.prophet import prophet_forecast
from pfa.analytics.xgboost import xgboost_forecast
from pfa.readwrite import frame_to_sql

logger = logging.get_logger(__file__)


def run_model_forecasts():
    forecasts = []
    logger.info("Prophet forecast")
    forecasts.append(loop_through_stocks(prophet_forecast))
    logger.info("XGBoost forecast")
    forecasts.append(loop_through_stocks(xgboost_forecast))
    logger.info("Bayesian ridge forecast")
    forecasts.append(loop_through_stocks(baysian_ridge_forecast))

    forecasts = pd.concat(forecasts)
    frame_to_sql(forecasts, "analytics_values")
