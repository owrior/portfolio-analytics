from sqlalchemy_views import CreateView
from sqlalchemy.orm import Query
from sqlalchemy.ext.declarative import declarative_base

from pfa.models.config import DateConfig
from pfa.models.config import MetricConfig
from pfa.models.config import StockConfig
from pfa.models.config import AnalyticsConfig
from pfa.models.values import AnalyticsValues
from pfa.models.values import StockValues
from pfa.id_cache import metric_id_cache

Base = declarative_base()
views = []


def ValidationMetricsView(Base):
    __tablename__ = "validation_metrics"


views.append(
    CreateView(
        ValidationMetricsView,
        Query(AnalyticsValues)
        .with_entities(
            DateConfig.date,
            StockConfig.stock,
            AnalyticsConfig.analysis,
            MetricConfig.metric,
            AnalyticsValues.value,
        )
        .join(DateConfig, DateConfig.date_id == AnalyticsValues.date_id)
        .join(StockConfig, StockConfig.stock_id == AnalyticsValues.stock_id)
        .join(
            AnalyticsConfig,
            AnalyticsConfig.analysis_id == AnalyticsValues.analytics_id,
        )
        .join(MetricConfig, MetricConfig.metric_id == AnalyticsValues.metric_id)
        .where(
            MetricConfig.metric_id.in_(
                [metric_id_cache.mean_abs_error, metric_id_cache.rmse]
            )
        ),
        or_replace=True,
    )
)
