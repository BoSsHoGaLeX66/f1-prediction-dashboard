import os
from dotenv import load_dotenv
import pandas as pd
import panel as pn
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from typing import Optional


pn.extension("plotly")


def _get_env_var(*keys: str, default: Optional[str] = None) -> Optional[str]:
    """Return the first populated environment variable for the provided keys."""
    for key in keys:
        value = os.getenv(key)
        if value:
            return value
    return default


def get_engine() -> Engine:
    """Build a SQLAlchemy engine using .env values, allowing .env to override OS vars."""
    load_dotenv(override=True)

    host = _get_env_var("POSTGRES_HOST", "HOST")
    user = _get_env_var("POSTGRES_USER", "USER")
    password = _get_env_var("POSTGRES_PASSWORD", "PASSWORD")
    db_name = _get_env_var("POSTGRES_DB", "DB_NAME", default="f1_prediction")
    port = _get_env_var("POSTGRES_PORT", "PORT", default="5432")

    missing = [
        key
        for key, val in {"host": host, "user": user, "password": password}.items()
        if not val
    ]
    if missing:
        raise RuntimeError(f"Missing required database env vars: {', '.join(missing)}")

    return create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db_name}")


def get_data() -> pd.DataFrame:
    engine = get_engine()
    return pd.read_sql("SELECT * FROM race_results", engine)


df = get_data()
pn.pane.DataFrame(df).servable()
