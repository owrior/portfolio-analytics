from sqlalchemy import Column
from sqlalchemy.types import Integer, Float

from pfa.models.base import Base


class PortfolioStockMap(Base):
    __tablename__ = "portfolio_stock_map"
    map_id = Column(Integer, primary_key=True)
    portfolio_id = Column(Integer)
    stock_id = Column(Integer)
    shares = Column(Float)
