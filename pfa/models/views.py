import sqlalchemy
from sqlalchemy.orm import Query

from pfa.models.config import DateConfig
from pfa.models.config import MetricConfig
from pfa.models.config import StockConfig
from pfa.models.config import AnalyticsConfig
from pfa.models.values import AnalyticsValues
from pfa.models.values import StockValues
from pfa.db import create_view_from_orm_query
from pfa.id_cache import metric_id_cache


def get_view_creation():
    return [*validation_metrics()]


def validation_metrics():
    query = get_values_table_with_labels(
        AnalyticsValues, [metric_id_cache.mean_abs_error, metric_id_cache.rmse]
    )
    return create_view_from_orm_query("validation_metrics", query)


def get_values_table_with_labels(
    Values, metric_ids: list, include_analytics: bool = True
):
    entities = [
        DateConfig.date,
        StockConfig.stock,
        MetricConfig.metric,
        Values.value,
    ]

    if include_analytics:
        entities.insert(3, AnalyticsConfig.analysis)

    query = (
        Query(Values)
        .with_entities(*entities)
        .join(DateConfig, DateConfig.date_id == Values.date_id)
        .join(StockConfig, StockConfig.stock_id == Values.stock_id)
        .join(MetricConfig, MetricConfig.metric_id == Values.metric_id)
    )

    if include_analytics:
        query = query.join(
            AnalyticsConfig,
            AnalyticsConfig.analysis_id == Values.analytics_id,
            isouter=True,
        )
    return query.where(MetricConfig.metric_id.in_([*metric_ids]))
