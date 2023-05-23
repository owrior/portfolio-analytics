from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy.orm import declarative_base
from sqlalchemy.types import Float
from sqlalchemy.types import Integer

from pfa.models.config import PortfolioConfig
from pfa.models.config import StockConfig

Base = declarative_base()


class PortfolioStockMap(Base):
    __tablename__ = "portfolio_stock_map"
    map_id = Column(Integer, primary_key=True)
    portfolio_id = Column(
        Integer,
        ForeignKey(
            PortfolioConfig.portfolio_id, ondelete="CASCADE", onupdate="CASCADE"
        ),
    )
    stock_id = Column(
        Integer,
        ForeignKey(StockConfig.stock_id, ondelete="CASCADE", onupdate="CASCADE"),
    )
    shares = Column(Float)
