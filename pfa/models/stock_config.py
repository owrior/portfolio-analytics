from sqlalchemy import Column
from sqlalchemy.types import Integer, String

from pfa.models.base import Base


class StockConfig(Base):
    __tablename__ = "stock_config"
    stock_id = Column(Integer, primary_key=True)
    currency_id = Column(Integer)
    stock = Column(String(30))
    yahoo_ticker = Column(String(30))
    industry = Column(String(50))
    description = Column(String(100))
