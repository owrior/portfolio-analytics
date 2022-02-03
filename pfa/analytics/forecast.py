import pandas as pd
from prefect.utilities import logging

from pfa.analytics.data_manipulation import loop_through_stocks
from pfa.readwrite import frame_to_sql

logger = logging.get_logger(__file__)


def run_model_forecasts():
    forecasts = []
    logger.info("Prophet forecast")
    logger.info("XGBoost forecast")

    forecasts = pd.concat(forecasts)
    frame_to_sql(forecasts, "analytics_values")
