from sqlalchemy import Column
from sqlalchemy.types import Integer, String

from pfa.models.base import Base


class ParameterConfig(Base):
    __tablename__ = "parameter_config"
    parameter_id = Column(Integer, primary_key=True)
    currency_id = Column(Integer)
    parameter = Column(String(30))
    url = Column(String(100))
    resource_name = Column(String(100))
    description = Column(String(100))
