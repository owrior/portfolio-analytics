from sqlalchemy import Column, ForeignKey
from sqlalchemy.types import Float, Integer
from sqlalchemy.ext.declarative import declarative_base

from pfa.models.config import DateConfig
from pfa.models.config import ParameterConfig
from pfa.models.config import StockConfig
from pfa.models.config import MetricConfig


Base = declarative_base()


class ParameterValues(Base):
    __tablename__ = "parameter_values"
    parameter_id = Column(
        Integer,
        ForeignKey(
            ParameterConfig.parameter_id, ondelete="CASCADE", onupdate="CASCADE"
        ),
        primary_key=True,
    )
    date_id = Column(
        Integer,
        ForeignKey(DateConfig.date_id, ondelete="CASCADE", onupdate="CASCADE"),
        primary_key=True,
    )
    value = Column(Float)


class StockValues(Base):
    __tablename__ = "stock_values"
    stock_id = Column(
        Integer,
        ForeignKey(StockConfig.stock_id, onupdate="CASCADE", ondelete="CASCADE"),
        primary_key=True,
    )
    date_id = Column(
        Integer,
        ForeignKey(DateConfig.date_id, onupdate="CASCADE", ondelete="CASCADE"),
        primary_key=True,
    )
    metric_id = Column(
        Integer,
        ForeignKey(MetricConfig.metric_id, onupdate="CASCADE", ondelete="CASCADE"),
        primary_key=True,
    )
    value = Column(Float)
