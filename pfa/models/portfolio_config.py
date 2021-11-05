from sqlalchemy import Column
from sqlalchemy.types import Integer, String

from pfa.models.base import Base


class PortfolioConfig(Base):
    __tablename__ = "portfolio_config"
    portfolio_id = Column(Integer, primary_key=True)
    name = Column(String(30))
    description = Column(String(100))
