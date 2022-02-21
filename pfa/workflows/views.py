from prefect import Flow
from prefect import task

from pfa.models.model import execute_view_creation


@task(log_stdout=True)
def create_views():
    execute_view_creation()


with Flow("Views") as flow:
    create_views()
