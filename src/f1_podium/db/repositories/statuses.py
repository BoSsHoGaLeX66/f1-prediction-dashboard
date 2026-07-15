"""Repository for the statuses table."""

from f1_podium.db.repositories.base import TableRepository


class StatusRepository(TableRepository):
    """Read and write race status reference data."""

    table_name = "statuses"
