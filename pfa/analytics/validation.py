import pandas as pd
from prefect.utilities import logging

from pfa.analytics.data_manipulation import loop_through_stocks
from pfa.analytics.prophet import validate_prophet_performance
from pfa.analytics.xgboost import validate_xgboost_performance
from pfa.readwrite import frame_to_sql

logger = logging.get_logger(__file__)


def run_model_validation():
    validation_results = []
    logger.info("Prophet validation")
    # validation_results.append(loop_through_stocks(validate_prophet_performance))
    logger.info("XGBoost validation")
    validation_results.append(loop_through_stocks(validate_xgboost_performance))

    validation_results = pd.concat(validation_results)
    frame_to_sql(validation_results, "analytics_values")
