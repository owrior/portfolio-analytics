from sqlalchemy import Column
from sqlalchemy.types import Integer, String, DateTime

from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class CurrencyConfig(Base):
    __tablename__ = "currency_config"
    currency_id = Column(Integer, primary_key=True)
    currency = Column(String(3))
    symbol = Column(String(1))


class DateConfig(Base):
    __tablename__ = "date_config"
    date_id = Column(Integer, primary_key=True)
    date = Column(DateTime)


class MetricConfig(Base):
    __tablename__ = "metric_config"
    metric_id = Column(Integer, primary_key=True)
    metric = Column(String(30))
    description = Column(String(100))


class ParameterConfig(Base):
    __tablename__ = "parameter_config"
    parameter_id = Column(Integer, primary_key=True)
    currency_id = Column(Integer)
    parameter = Column(String(30))
    url = Column(String(100))
    resource_name = Column(String(100))
    description = Column(String(100))


class PortfolioConfig(Base):
    __tablename__ = "portfolio_config"
    portfolio_id = Column(Integer, primary_key=True)
    portfolio = Column(String(30))
    description = Column(String(100))


class StockConfig(Base):
    __tablename__ = "stock_config"
    stock_id = Column(Integer, primary_key=True)
    currency_id = Column(Integer)
    stock = Column(String(30))
    yahoo_ticker = Column(String(30))
    industry = Column(String(50))
    description = Column(String(100))
