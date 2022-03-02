from prefect.utilities import logging

from pfa.db import execute_query
from pfa.models.config import Base as config_Base
from pfa.models.map import Base as map_Base
from pfa.models.values import Base as values_Base
from pfa.models.views import get_view_creation

logger = logging.get_logger(__file__)


def create_database_from_model(engine):
    config_Base.metadata.create_all(engine)
    logger.debug("Created database metadata for config")
    values_Base.metadata.create_all(engine)
    logger.debug("Created database metadata for values")
    map_Base.metadata.create_all(engine)
    logger.debug("Created database metadata for mapping")
    logger.info("Created database metadata")


def execute_view_creation():
    [execute_query(view) for view in get_view_creation()]
    logger.info("Created views")
