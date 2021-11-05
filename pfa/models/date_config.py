from sqlalchemy import Column
from sqlalchemy.types import DateTime, Integer

from pfa.models.base import Base


class DateConfig(Base):
    __tablename__ = "date_config"
    date_id = Column(Integer, primary_key=True)
    date = Column(DateTime)
