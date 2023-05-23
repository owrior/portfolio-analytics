from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy.orm import declarative_base
from sqlalchemy.types import Float
from sqlalchemy.types import Integer

from pfa.models.config import AnalyticsConfig
from pfa.models.config import DateConfig
from pfa.models.config import MetricConfig
from pfa.models.config import ParameterConfig
from pfa.models.config import StockConfig

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


class AnalyticsValues(Base):
    __tablename__ = "analytics_values"
    forecast_date_id = Column(
        Integer,
        ForeignKey(DateConfig.date_id, onupdate="CASCADE", ondelete="CASCADE"),
        primary_key=True,
    )
    analytics_id = Column(
        Integer,
        ForeignKey(AnalyticsConfig.analysis_id, onupdate="CASCADE", ondelete="CASCADE"),
        primary_key=True,
    )
    stock_id = Column(
        Integer,
        ForeignKey(StockConfig.stock_id, onupdate="CASCADE", ondelete="CASCADE"),
        primary_key=True,
    )
    metric_id = Column(
        Integer,
        ForeignKey(MetricConfig.metric_id, onupdate="CASCADE", ondelete="CASCADE"),
        primary_key=True,
    )
    date_id = Column(
        Integer,
        ForeignKey(DateConfig.date_id, onupdate="CASCADE", ondelete="CASCADE"),
        primary_key=True,
    )
    value = Column(Float)
