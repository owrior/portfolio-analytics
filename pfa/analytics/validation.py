from pfa.analytics.prophet import loop_through_stock_values
from pfa.analytics.prophet import validate_prophet_performance
from pfa.readwrite import frame_to_sql


def run_model_validation():
    prophet_validation = loop_through_stock_values(validate_prophet_performance)

    frame_to_sql(prophet_validation, "analytics_values")
