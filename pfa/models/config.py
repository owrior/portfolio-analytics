from sqlalchemy import Column
from sqlalchemy.orm import declarative_base
from sqlalchemy.types import Boolean, DateTime, Integer, String

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
    validation = Column(Boolean)
    description = Column(String(250))


class ParameterConfig(Base):
    __tablename__ = "parameter_config"
    parameter_id = Column(Integer, primary_key=True)
    currency = Column(String(5))
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


class AnalyticsConfig(Base):
    __tablename__ = "analytics_config"
    analysis_id = Column(Integer, primary_key=True)
    analysis = Column(String(30))
    description = Column(String(100))
