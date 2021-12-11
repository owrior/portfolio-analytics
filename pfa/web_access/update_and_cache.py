import pandas as pd
from sqlalchemy.sql.sqltypes import Date
from pfa.readwrite import read_sql

from sqlalchemy.orm import Query
import sqlalchemy as sqa

from pfa.models.stock_config import StockConfig
from pfa.models.stock_values import StockValues
from pfa.models.date_config import DateConfig


def get_most_recent_stock_dates():
    stock_dates = read_sql(
        Query(StockConfig)
        .with_entities(
            StockConfig.stock_id,
            StockConfig.yahoo_ticker,
            sqa.func.max(StockValues.date_id).label("date_id"),
            DateConfig.date,
        )
        .join(StockValues, StockConfig.stock_id == StockValues.stock_id, isouter=True)
        .group_by(StockConfig.stock_id)
        .join(DateConfig, StockValues.date_id == DateConfig.date_id, isouter=True)
    )
    return stock_dates.loc[:, ["stock_id", "yahoo_ticker", "date"]]
