from pfa.db_admin import initialise_database
from pfa.web_access.yahoo_finance import populate_yahoo_stock_values

initialise_database()
populate_yahoo_stock_values()
