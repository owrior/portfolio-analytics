import sqlalchemy as sqa
from sqlalchemy.orm import Query

from pfa.db import create_view_from_orm_query
from pfa.id_cache import metric_id_cache
from pfa.models.config import AnalyticsConfig, DateConfig, MetricConfig, StockConfig
from pfa.models.values import AnalyticsValues, StockValues


def get_view_creation():
    return [*validation_metrics(), *historical_adj_close(), *forecasts()]


def validation_metrics():
    query = get_values_table_with_labels(
        AnalyticsValues,
        [metric_id_cache.rmse, metric_id_cache.rmsle],
    )
    return create_view_from_orm_query("validation_metrics", query)


def historical_adj_close():
    query = get_values_table_with_labels(
        StockValues, [metric_id_cache.adj_close], include_analytics=False
    )
    return create_view_from_orm_query("historical_adj_close", query)


def forecasts():
    query = sqa.union_all(
        get_values_table_with_labels(
            AnalyticsValues, [metric_id_cache.adj_close, metric_id_cache.log_return]
        ),
        get_values_table_with_labels(
            StockValues,
            [metric_id_cache.adj_close, metric_id_cache.log_return],
            include_analytics=False,
        ),
    )
    return create_view_from_orm_query("forecasts", query)


def get_values_table_with_labels(
    Values, metric_ids: list, include_analytics: bool = True
):
    entities = [
        DateConfig.date.label("date"),
        StockConfig.stock.label("stock"),
        AnalyticsConfig.analysis.label("analysis")
        if include_analytics
        else sqa.sql.expression.literal("Actual Data").label("analysis"),
        MetricConfig.metric.label("metric"),
        Values.value.label("value"),
    ]

    query = (
        Query(Values)
        .with_entities(*entities)
        .join(DateConfig, DateConfig.date_id == Values.date_id)
        .join(StockConfig, StockConfig.stock_id == Values.stock_id)
        .join(MetricConfig, MetricConfig.metric_id == Values.metric_id)
    )

    if include_analytics:
        query = query.join(
            AnalyticsConfig, AnalyticsConfig.analysis_id == Values.analytics_id
        )
    return query.where(MetricConfig.metric_id.in_([*metric_ids]))
