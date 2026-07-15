"""Shared repository behavior for database tables."""

import pandas as pd

from f1_podium.db.connection import DatabaseConnection


class TableRepository:
    """Base repository that owns table-level read and append behavior."""

    table_name: str

    def __init__(self, connection: DatabaseConnection):
        """Store the database connection used by this repository."""
        self.connection = connection

    def get_all(self) -> pd.DataFrame:
        """Return every row from the repository table."""
        return self.connection.read_table(self.table_name)

    def append(self, data: pd.DataFrame, conn=None) -> None:
        """Append rows to the repository table."""
        self.connection.append_dataframe(self.table_name, data, conn=conn)
