from sqlalchemy import Column
from sqlalchemy.types import Integer, String

from pfa.models.base import Base


class MetricConfig(Base):
    __tablename__ = "metric_config"
    metric_id = Column(Integer, primary_key=True)
    metric = Column(String(30))
    description = Column(String(100))
