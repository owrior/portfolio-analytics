import datetime as dt

from prefect.utilities import logging
from sqlalchemy.orm import Query

from pfa.models.config import AnalyticsConfig
from pfa.models.config import DateConfig
from pfa.models.config import MetricConfig
from pfa.readwrite import read_sql

logger = logging.get_logger(__file__)


class IDCache:
    """
    Class cache ids from the database which can be referenced as a property.
    """

    id_column = None
    label_column = None
    table = None
    dataframe = None

    def init_df(self):
        if self.dataframe is None:
            self.dataframe = read_sql(Query(self.table))

    def get_id(self, label):
        self.init_df()
        return int(
            self.dataframe.loc[
                self.dataframe[self.label_column] == label, self.id_column
            ].iloc[0]
        )


class MetricIDCache(IDCache):
    id_column = "metric_id"
    label_column = "metric"
    table = MetricConfig
    _validation_metrics = None

    @property
    def validation_metrics(self):
        self.init_df()
        if self._validation_metrics is None:
            self._validation_metrics = self.dataframe.loc[
                self.dataframe["validation"], "metric_id"
            ].to_list()
        return self._validation_metrics

    @property
    def adj_close(self):
        return self.get_id("Adj Close")

    @property
    def volume(self):
        return self.get_id("Volume")

    @property
    def log_return(self):
        return self.get_id("Log return")

    @property
    def prediction(self):
        return self.get_id("Prediction")

    @property
    def prediction_upper(self):
        return self.get_id("Prediction Upper")

    @property
    def prediction_lower(self):
        return self.get_id("Prediction Lower")

    @property
    def mean_error(self):
        return self.get_id("Mean error")

    @property
    def mean_abs_error(self):
        return self.get_id("Mean (abs) error")

    @property
    def rmse(self):
        return self.get_id("RMSE")

    @property
    def rmsle(self):
        return self.get_id("RMSLE")

    @property
    def smape(self):
        return self.get_id("sMAPE")


class AnalyticsIDCache(IDCache):
    id_column = "analysis_id"
    label_column = "analysis"
    table = AnalyticsConfig

    @property
    def prophet(self):
        return self.get_id("Prophet")

    @property
    def xgboost(self):
        return self.get_id("XGBoost Regression")

    @property
    def bayesian_ridge(self):
        return self.get_id("Bayesian Ridge Regression")


class DateIDCache(IDCache):
    id_column = "date_id"
    label_column = "date"
    table = DateConfig
    current_id = None

    @property
    def todays_id(self):
        self.init_df()
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
