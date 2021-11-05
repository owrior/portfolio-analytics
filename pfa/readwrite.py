import pandas as pd

from pfa.db import get_engine


def frame_to_sql(df: pd.DataFrame, table_name: str, clear_table: bool = False) -> None:
    df.to_sql(
        table_name, con=get_engine(), index=False, if_exists="append", method="multi"
    )


def read_sql(query) -> pd.DataFrame:
    return pd.read_sql(query.statement, get_engine())
