from prefect import Flow, task

from pfa.analytics.prophet import run_prophet_forcast_for_stocks


@task(log_stdout=True)
def run_analytics():
    run_prophet_forcast_for_stocks()


with Flow("Analytics") as flow:
    run_analytics()
