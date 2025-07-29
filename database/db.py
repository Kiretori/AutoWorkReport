from sqlalchemy import create_engine
from database import DB_URL

_engine = None


def get_engine(db_url=DB_URL):
    global _engine
    if _engine is None:
        if db_url is None:
            raise ValueError("DB URL must be provided the first time.")
        _engine = create_engine(db_url, future=True)
    return _engine
