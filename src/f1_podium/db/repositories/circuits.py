"""Repository for the circuits table."""

from f1_podium.db.repositories.base import TableRepository


class CircuitRepository(TableRepository):
    """Read circuit reference data."""

    table_name = "circuits"
