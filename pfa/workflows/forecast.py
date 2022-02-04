from prefect import Flow
from prefect import task

from pfa.analytics.forecast import run_model_forecasts


@task(log_stdout=True)
def run_analytics():
    run_model_forecasts()


with Flow("Forecast") as flow:
    run_analytics()
