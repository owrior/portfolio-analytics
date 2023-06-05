import pandas as pd
from prefect.utilities import logging

from pfa.db import get_engine

logger = logging.get_logger(__file__)


def frame_to_sql(df: pd.DataFrame, table_name: str) -> None:
    """
    Insert dataframe to sql.
    """
    if df.empty:
        logger.debug("Did not insert empty dataframe")
    else:
        df.round(decimals=4).to_sql(
            table_name,
            chunksize=10000,
            con=get_engine(),
            index=False,
            if_exists="append",
            method="multi",
        )
    logger.info(f"Inserted dataframe, shape: {df.shape} to {table_name}")


def read_sql(query, text=False) -> pd.DataFrame:
    """
    Read a sql query to a pandas dataframe.
    """
    return pd.read_sql(query if text else query.statement, get_engine())
