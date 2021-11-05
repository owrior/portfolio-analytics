from sqlalchemy import Column
from sqlalchemy.types import Integer, DateTime

from pfa.models.base import Base


class Date(Base):
    __tablename__ = "date"
    date_id = Column(Integer, primary_key=True)
    date = Column(DateTime)
