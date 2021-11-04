from sqlalchemy import Column
from sqlalchemy.types import Integer, Float

from pfa.models.base import Base


class ParameterValues(Base):
    __tablename__ = "parameter_values"
    parameter_id = Column(Integer, primary_key=True)
    date_id = Column(Integer)
    value = Column(Float)
