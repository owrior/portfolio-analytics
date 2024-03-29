from prefect import Flow

from pfa.db_admin import initialise_database
from pfa.models.model import execute_view_creation
from pfa.web_access.datahub import populate_datahub_parameter_values
from pfa.web_access.update import (
    get_most_recent_datahub_parameter_dates,
    get_most_recent_stock_dates,
)
from pfa.web_access.yahoo_finance import populate_yahoo_stock_values

with Flow("Initialise") as flow:
    initialised_database = initialise_database()

    execute_view_creation.set_upstream(initialised_database)

    stock_dates = get_most_recent_stock_dates.set_upstream(initialised_database)
    populated_yahoo_stock_values = populate_yahoo_stock_values.map(stock_dates)

    parameter_dates = get_most_recent_datahub_parameter_dates.set_upstream(
        initialised_database
    )
    populated_datahub_parameter_values = populate_datahub_parameter_values.map(
        parameter_dates
    )
