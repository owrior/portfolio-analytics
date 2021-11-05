from sqlalchemy import Column
from sqlalchemy.types import Integer, String

from pfa.models.base import Base


class CurrencyConfig(Base):
    __tablename__ = "currency_config"
    currency_id = Column(Integer, primary_key=True)
    currency = Column(String(3))
    symbol = Column(String(1))
