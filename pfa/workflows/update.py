from prefect import Flow
from prefect import task

from pfa.web_access.datahub import populate_datahub_parameter_values
from pfa.web_access.update import get_most_recent_datahub_parameter_dates
from pfa.web_access.update import get_most_recent_stock_dates
from pfa.web_access.yahoo_finance import populate_yahoo_stock_values

with Flow("Update") as flow:
    stock_dates = get_most_recent_stock_dates
    populated_yahoo_stock_values = populate_yahoo_stock_values.map(stock_dates)

    parameter_dates = get_most_recent_datahub_parameter_dates
    populated_datahub_parameter_values = populate_datahub_parameter_values.map(
        parameter_dates
    )
