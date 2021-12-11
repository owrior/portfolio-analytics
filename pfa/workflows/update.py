from prefect import Flow, task

from pfa.db_admin import initialise_database
from pfa.web_access.datahub import populate_datahub_parameter_values
from pfa.web_access.yahoo_finance import populate_yahoo_stock_values


@task()
def update_database():
    populate_yahoo_stock_values()
    populate_datahub_parameter_values()


with Flow("Update") as flow:
    update_database()
