from pfa.models.config import Base as config_Base
from pfa.models.map import Base as map_Base
from pfa.models.values import Base as values_Base

from prefect.utilities import logging

logger = logging.get_logger(__file__)


def create_database_from_model(engine):
    config_Base.metadata.create_all(engine)
    logger.debug("Created database metadata for config")
    values_Base.metadata.create_all(engine)
    logger.debug("Created database metadata for values")
    map_Base.metadata.create_all(engine)
    logger.debug("Created database metadata for mapping")
    logger.info("Created database metadata")
