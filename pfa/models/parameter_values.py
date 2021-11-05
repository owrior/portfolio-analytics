from sqlalchemy import Column, ForeignKey
from sqlalchemy.types import Float, Integer

from pfa.models.base import Base
from pfa.models.date_config import DateConfig
from pfa.models.parameter_config import ParameterConfig


class ParameterValues(Base):
    __tablename__ = "parameter_values"
    parameter_id = Column(
        Integer,
        ForeignKey(
            ParameterConfig.parameter_id, ondelete="CASCADE", onupdate="CASCADE"
        ),
        primary_key=True,
    )
    date_id = Column(
        Integer,
        ForeignKey(DateConfig.date_id, ondelete="CASCADE", onupdate="CASCADE"),
        primary_key=True,
    )
    value = Column(Float)
