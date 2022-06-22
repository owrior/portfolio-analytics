import sqlalchemy as sqa
from prefect.utilities import logging
from sqlalchemy.orm import Query
from sqlalchemy_views import CreateView
from sqlalchemy_views import DropView

logger = logging.get_logger(__file__)

PDB = "PFA"


def get_engine(db_name: str = None) -> sqa.engine:
    if not db_name:
        db_name = PDB
    url = f"postgres+psycopg2cffi://postgres:postgrespw@localhost:49153"
    return sqa.create_engine(url)


def execute_query(query: Query) -> None:
    with get_engine().begin() as conn:
        conn.execute(query)


def create_view_from_orm_query(view_name: str, query: Query):
    if isinstance(query, sqa.sql.selectable.CompoundSelect):
        query = str(query.compile(compile_kwargs={"literal_binds": True}))
    else:
        query = str(query.statement.compile(compile_kwargs={"literal_binds": True}))
    return [
        DropView(sqa.Table(view_name, sqa.MetaData()), if_exists=True),
        CreateView(
            sqa.Table(view_name, sqa.MetaData()),
            sqa.text(query),
        ),
    ]
