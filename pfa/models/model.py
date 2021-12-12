from pfa.models.config import Base as config_Base
from pfa.models.values import Base as values_Base
from pfa.models.map import Base as map_Base


def create_database_from_model(engine):
    config_Base.metadata.create_all(engine)
    values_Base.metadata.create_all(engine)
    map_Base.metadata.create_all(engine)
