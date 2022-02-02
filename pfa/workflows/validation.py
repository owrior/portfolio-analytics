from prefect import Flow
from prefect import task

from pfa.analytics.validation import run_model_validation


@task(log_stdout=True)
def run_analytics():
    run_model_validation()


with Flow("Validation") as flow:
    run_analytics()
