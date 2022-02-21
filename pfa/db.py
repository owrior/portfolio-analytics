from pathlib import Path

import sqlalchemy as sqa
from prefect.utilities import logging
from sqlalchemy.orm import Query
from sqlalchemy_views import CreateView
from sqlalchemy_views import DropView

logger = logging.get_logger(__file__)

ENGINE_CACHE = {}
PDB = "PFA_TEST_NEW"
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


def execute_query(query: Query) -> None:
    with get_engine().begin() as conn:
        conn.execute(query)


def create_view_from_orm_query(view_name: str, query: Query):
    return [
        DropView(sqa.Table(view_name, sqa.MetaData()), if_exists=True),
        CreateView(
            sqa.Table(view_name, sqa.MetaData()),
            sqa.text(
                str(query.statement.compile(compile_kwargs={"literal_binds": True}))
            ),
        ),
    ]
