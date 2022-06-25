import prefect
import sqlalchemy as sqa
from sqlalchemy.orm import Query

from pfa.models.config import DateConfig
from pfa.models.config import ParameterConfig
from pfa.models.config import StockConfig
from pfa.models.values import ParameterValues
from pfa.models.values import StockValues
from pfa.readwrite import read_sql


@prefect.task
def get_most_recent_stock_dates() -> dict:
    stock_dates = read_sql(
        Query(StockConfig)
        .with_entities(
            StockConfig.stock_id.label("stock_id"),
            StockConfig.yahoo_ticker.label("yahoo_ticker"),
            sqa.func.max(DateConfig.date).label("date"),
        )
        .join(StockValues, StockConfig.stock_id == StockValues.stock_id, isouter=True)
        .join(DateConfig, StockValues.date_id == DateConfig.date_id, isouter=True)
        .group_by(StockConfig.stock_id)
    )
    return [
        {"yahoo_ticker": sd.yahoo_ticker, "stock_id": sd.stock_id, "date": sd.date}
        for sd in stock_dates.itertuples()
        if sd.yahoo_ticker is not None
    ]


def get_most_recent_datahub_parameter_dates():
    parameter_config = read_sql(
        Query(ParameterConfig)
        .with_entities(
            ParameterConfig.parameter_id.label("parameter_id"),
            ParameterConfig.url.label("url"),
            ParameterConfig.resource_name.label("resource_name"),
            sqa.func.max(DateConfig.date).label("date"),
        )
        .filter(ParameterConfig.url.like("%datahub.io%"))
        .join(
            ParameterValues,
            ParameterConfig.parameter_id == ParameterValues.parameter_id,
            isouter=True,
        )
        .join(DateConfig, ParameterValues.date_id == DateConfig.date_id, isouter=True)
        .group_by(ParameterConfig.parameter_id)
    )
    return parameter_config[["parameter_id", "url", "resource_name", "date"]]
