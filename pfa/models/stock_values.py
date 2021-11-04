from sqlalchemy import Column
from sqlalchemy.types import Integer, Float

from pfa.models.base import Base


class StockValues(Base):
    __tablename__ = "stock_values"
    stock_id = Column(Integer, primary_key=True)
    date_id = Column(Integer)
    value = Column(Float)
