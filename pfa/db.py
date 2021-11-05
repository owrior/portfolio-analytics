import sqlalchemy as sqa
from pathlib import Path

engines = {}
PDB = "PFA_TEST"
SQLITE_FOLDER = Path(__file__).parents[2] / "sqlite"


def get_engine(db_name: str = None) -> sqa.engine:
    if not db_name:
        db_name = PDB

    if db_name not in engines:
        db_path = SQLITE_FOLDER / f"{db_name}.sqlite3"
        url = f"sqlite:///{db_path}"
        engines[db_name] = sqa.create_engine(url)

    return engines[db_name]
