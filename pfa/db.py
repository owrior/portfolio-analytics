from pathlib import Path

import sqlalchemy as sqa

from prefect.utilities import logging

logger = logging.get_logger(__file__)

ENGINE_CACHE = {}
PDB = "PFA_TEST"
SQLITE_FOLDER = Path(__file__).parents[2] / "sqlite"
SQLITE_FOLDER.mkdir(parents=True, exist_ok=True)


def get_engine(db_name: str = None) -> sqa.engine:
    if not db_name:
        db_name = PDB

    if db_name not in ENGINE_CACHE:
        db_path = SQLITE_FOLDER / f"{db_name}.sqlite3"
        url = f"sqlite:///{db_path}"
        ENGINE_CACHE[db_name] = sqa.create_engine(url)
        logger.debug(f"Created database engine: {url}")

    return ENGINE_CACHE[db_name]
