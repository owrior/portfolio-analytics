from sqlalchemy import Column, ForeignKey
from sqlalchemy.types import Float, Integer

from pfa.models.base import Base
from pfa.models.date_config import DateConfig
from pfa.models.metric_config import MetricConfig
from pfa.models.stock_config import StockConfig


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
