import datetime as dt

from sqlalchemy.orm import Query

from pfa.models.config import AnalyticsConfig, DateConfig, MetricConfig
from pfa.readwrite import read_sql


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


class DateIDCache(IDCache):
    id_column = "date_id"
    label_column = "date"
    table = DateConfig
    current_id = None

    @property
    def todays_id(self):
        if self.current_id is None:
            self.current_id = int(
                self.dataframe.loc[
                    self.dataframe["date"].dt.date == dt.date.today(), "date_id"
                ].iloc[0]
            )
        return self.current_id


metric_id_cache = MetricIDCache()
analytics_id_cache = AnalyticsIDCache()
date_id_cache = DateIDCache()