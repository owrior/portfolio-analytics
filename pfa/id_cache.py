from sqlalchemy.orm import Query

from pfa.readwrite import read_sql
from pfa.models.config import MetricConfig
from pfa.models.config import AnalyticsConfig


class IDCache:
    id_column = None
    label_column = None
    table = None

    def __init__(self) -> None:
        self.dataframe = read_sql(Query(self.table))

    def get_id(self, label):
        return int(
            self.dataframe.loc[
                self.dataframe[self.label_column] == label, self.id_column
            ].iloc[0]
        )


class MetricIDCache(IDCache):
    id_column = "metric_id"
    label_column = "metric"
    table = MetricConfig

    @property
    def adj_close(self):
        return self.get_id("Adj Close")

    @property
    def prediction(self):
        return self.get_id("Prediction")

    @property
    def prediction_upper(self):
        return self.get_id("Prediction Upper")

    @property
    def prediction_lower(self):
        return self.get_id("Prediction Lower")


class AnalyticsIDCache(IDCache):
    id_column = "analysis_id"
    label_column = "analysis"
    table = AnalyticsConfig

    @property
    def prophet(self):
        return self.get_id("Prophet")


metric_id_cache = MetricIDCache()
analytics_id_cache = AnalyticsIDCache()
