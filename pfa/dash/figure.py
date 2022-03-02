import sqlalchemy as sqa
import pandas as pd
from pfa.readwrite import read_sql


def except_missing_db(element_fn, exception_return="Database error"):
    try:
        return element_fn
    except sqa.exc.OperationalError:
        return exception_return


def get_dropdown_options(view_name, column_name):
    options = except_missing_db(
        read_sql(f"SELECT DISTINCT {column_name} FROM {view_name}", text=True)
    )
    if isinstance(options, pd.DataFrame):
        return options[column_name].to_list()
    else:
        return options


def get_date_range(view_name):
    minimum = except_missing_db(
        read_sql(f"SELECT MIN(date) FROM {view_name}", text=True),
        exception_return=pd.DataFrame([(pd.Timestamp(year=1990, month=1, day=1))]),
    )
    maximum = except_missing_db(
        read_sql(f"SELECT MAX(date) FROM {view_name}", text=True),
        exception_return=pd.DataFrame([(pd.Timestamp(year=2050, month=1, day=1))]),
    )
    return {
        "min": pd.to_datetime(minimum.iloc[0, 0]).date(),
        "max": pd.to_datetime(maximum.iloc[0, 0]).date(),
    }
