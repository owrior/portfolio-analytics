import pandas as pd

from pfa.db import get_engine

from prefect.utilities import logging

logger = logging.get_logger(__file__)


def frame_to_sql(df: pd.DataFrame, table_name: str, clear_table: bool = False) -> None:
    if df.empty:
        logger.debug("Did not insert empty dataframe")
    else:
        df.to_sql(
            table_name,
            con=get_engine(),
            index=False,
            if_exists="append",
            method="multi",
        )
    logger.info(f"Inserted dataframe shape: {df.shape} to {table_name}")


def read_sql(query) -> pd.DataFrame:
    return pd.read_sql(query.statement, get_engine())
