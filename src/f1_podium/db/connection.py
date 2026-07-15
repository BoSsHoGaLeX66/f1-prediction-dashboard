"""Database connection management for repository classes."""

import asyncio
import inspect
import os
from contextlib import contextmanager
from typing import Optional

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


class DatabaseConnection:
    """Create and share SQLAlchemy database connections for repositories."""

    def __init__(self, engine: Engine):
        """Store the SQLAlchemy engine used by repository instances."""
        self.engine = engine

    @classmethod
    async def from_prefect_block(cls, block_name: str = "postgresdb"):
        """Build a connection from a Prefect PostgreSQL connector block."""
        from f1_podium.blocks.postgresql_conn import PostgresqlConnector

        block = PostgresqlConnector.load(block_name)
        if inspect.isawaitable(block):
            block = await block
        return cls(block.get_engine())

    @classmethod
    def from_prefect_block_sync(cls, block_name: str = "postgresdb"):
        """Build a connection from a Prefect block in synchronous tasks."""
        from f1_podium.blocks.postgresql_conn import PostgresqlConnector

        block = PostgresqlConnector.load(block_name)
        if inspect.isawaitable(block):
            block = asyncio.run(block)
        return cls(block.get_engine())

    @classmethod
    def from_env(cls):
        """Build a connection from database environment variables."""
        load_dotenv(override=True)

        host = _get_env_var("POSTGRES_HOST", "HOST")
        user = _get_env_var("POSTGRES_USER", "USER")
        password = _get_env_var("POSTGRES_PASSWORD", "PASSWORD")
        db_name = _get_env_var("POSTGRES_DB", "DB_NAME", default="f1_prediction")
        port = _get_env_var("POSTGRES_PORT", "PORT", default="5432")

        missing = [
            key
            for key, value in {"host": host, "user": user, "password": password}.items()
            if not value
        ]
        if missing:
            raise RuntimeError(
                f"Missing required database env vars: {', '.join(missing)}"
            )

        return cls(create_postgres_engine(user, password, host, port, db_name))

    @contextmanager
    def transaction(self):
        """Open a database transaction for coordinated repository writes."""
        with self.engine.begin() as conn:
            yield conn

    def read_sql(self, query: str, params: Optional[dict] = None) -> pd.DataFrame:
        """Read a SQL query into a DataFrame."""
        return pd.read_sql(query, self.engine, params=params)

    def read_table(self, table_name: str) -> pd.DataFrame:
        """Read all rows from a database table into a DataFrame."""
        return self.read_sql(f"SELECT * FROM {table_name}")

    def append_dataframe(self, table_name: str, data: pd.DataFrame, conn=None) -> None:
        """Append a DataFrame to a table when the DataFrame has rows."""
        if data is None or data.empty:
            return

        target = conn if conn is not None else self.engine
        data.to_sql(table_name, target, if_exists="append", index=False, method="multi")


def _get_env_var(*keys: str, default: Optional[str] = None) -> Optional[str]:
    """Return the first populated environment variable for the provided keys."""
    for key in keys:
        value = os.getenv(key)
        if value:
            return value
    return default


def create_postgres_engine(
    user: str, password: str, host: str, port: int | str, db_name: str
) -> Engine:
    """Create a SQLAlchemy engine for the configured PostgreSQL database."""
    return create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db_name}")
