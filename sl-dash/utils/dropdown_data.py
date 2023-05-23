from typing import List

from pfa.readwrite import read_sql


def get_dropdown_options(
    view_name: str, column_name: str, check_in: str = None
) -> List[str]:
    options = read_sql(f"SELECT DISTINCT {column_name} FROM {view_name}", text=True)

    if check_in:
        check = read_sql(f"SELECT DISTINCT {column_name} FROM {check_in}", text=True)
        if not check.empty:
            options = options.loc[options[column_name].isin(check[column_name]), :]
    return options[column_name].to_list()
