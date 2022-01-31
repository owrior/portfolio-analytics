import sqlalchemy as sqa
from sqlalchemy.orm import Query

from pfa.models.config import DateConfig
from pfa.models.config import ParameterConfig
from pfa.models.config import StockConfig
from pfa.models.values import ParameterValues
from pfa.models.values import StockValues
from pfa.readwrite import read_sql


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
    return stock_dates[["stock_id", "yahoo_ticker", "date"]]


def get_most_recent_datahub_parameter_dates():
    parameter_config = read_sql(
        Query(ParameterConfig)
        .with_entities(
            ParameterConfig.parameter_id,
            ParameterConfig.url,
            ParameterConfig.resource_name,
            sqa.func.max(ParameterValues.date_id).label("date_id"),
            DateConfig.date,
        )
        .filter(ParameterConfig.url.like("%datahub.io%"))
        .join(
            ParameterValues,
            ParameterConfig.parameter_id == ParameterValues.parameter_id,
            isouter=True,
        )
        .group_by(ParameterConfig.parameter_id)
        .join(DateConfig, ParameterValues.date_id == DateConfig.date_id, isouter=True)
    )
    return parameter_config[["parameter_id", "url", "resource_name", "date"]]
