from prefect import Flow
from prefect import task

from pfa.db_admin import initialise_database
from pfa.models.model import execute_view_creation
from pfa.web_access.datahub import populate_datahub_parameter_values
from pfa.web_access.yahoo_finance import populate_yahoo_stock_values


@task(log_stdout=True)
def create_and_populate_db():
    initialise_database()
    populate_yahoo_stock_values()
    populate_datahub_parameter_values()
    execute_view_creation()


with Flow("Initialise") as flow:
    create_and_populate_db()
