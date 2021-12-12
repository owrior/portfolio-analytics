from sqlalchemy import Column
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import Integer, String

Base = declarative_base()


class CurrencyConfig(Base):
    __tablename__ = "currency_config"
    currency_id = Column(Integer, primary_key=True)
    currency = Column(String(3))
    symbol = Column(String(1))
