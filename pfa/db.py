from pathlib import Path

import sqlalchemy as sqa

ENGINE_CACHE = {}
PDB = "PFA_TEST"
SQLITE_FOLDER = Path(__file__).parents[2] / "sqlite"


def get_engine(db_name: str = None) -> sqa.engine:
    if not db_name:
        db_name = PDB

    if db_name not in ENGINE_CACHE:
        db_path = SQLITE_FOLDER / f"{db_name}.sqlite3"
        url = f"sqlite:///{db_path}"
        ENGINE_CACHE[db_name] = sqa.create_engine(url)

    return ENGINE_CACHE[db_name]
