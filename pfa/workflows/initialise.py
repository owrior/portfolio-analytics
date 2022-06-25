from prefect import Flow
from prefect import task

from pfa.db_admin import initialise_database
from pfa.web_access.datahub import populate_datahub_parameter_values
from pfa.web_access.update import get_most_recent_stock_dates
from pfa.web_access.yahoo_finance import populate_yahoo_stock_values


@task(log_stdout=True)
def create_and_populate_db():
    populate_yahoo_stock_values()
    populate_datahub_parameter_values()


with Flow("Initialise") as flow:
    initialised_database = initialise_database()

    stock_dates = get_most_recent_stock_dates.set_upstream(initialised_database)
    populated_yahoo_stock_values = populate_yahoo_stock_values.map(stock_dates)
