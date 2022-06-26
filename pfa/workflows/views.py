from prefect import Flow

from pfa.models.model import execute_view_creation

with Flow("Views") as flow:
    execute_view_creation()
