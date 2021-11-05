from pfa.models.base import Base
from pfa.models.currency_config import CurrencyConfig
from pfa.models.date_config import DateConfig
from pfa.models.metric_config import MetricConfig
from pfa.models.parameter_config import ParameterConfig
from pfa.models.parameter_values import ParameterValues
from pfa.models.portfolio_config import PortfolioConfig
from pfa.models.portfolio_stock_map import PortfolioStockMap
from pfa.models.stock_config import StockConfig
from pfa.models.stock_values import StockValues

currency_config = CurrencyConfig()
date_config = DateConfig()
metric_config = MetricConfig()
parameter_config = ParameterConfig()
parameter_values = ParameterValues()
portfolio_config = PortfolioConfig()
portfolio_stock_map = PortfolioStockMap()
stock_config = StockConfig()
stock_values = StockValues()


def create_database_from_model(engine):
    Base.metadata.create_all(engine)
