import datetime as dt

import pandas as pd
import sqlalchemy as sqa

from pfa.readwrite import read_sql


def except_missing_db(element_fn, exception_return="Database error"):
    try:
        return element_fn
    except sqa.exc.OperationalError:
        return exception_return


def get_dropdown_options(view_name: str, column_name: str, check_in: str = None):
    options = except_missing_db(
        read_sql(f"SELECT DISTINCT {column_name} FROM {view_name}", text=True),
        exception_return=["Error"],
    )
    if check_in:
        check = except_missing_db(
            read_sql(f"SELECT DISTINCT {column_name} FROM {check_in}", text=True),
            exception_return=["Error"],
        )
        if not check.empty:
            options = options.loc[options[column_name].isin(check[column_name]), :]
    return options[column_name].to_list()


def get_date_kwargs(view_name):
    return {
        "start_date": pd.Timestamp(dt.date.today() - dt.timedelta(days=90)),
        "end_date": pd.Timestamp(dt.date.today() + dt.timedelta(days=90)),
        "min_date_allowed": pd.Timestamp(dt.datetime(year=1990, month=1, day=1)),
        "max_date_allowed": pd.Timestamp(dt.datetime(year=2050, month=1, day=1)),
    }
