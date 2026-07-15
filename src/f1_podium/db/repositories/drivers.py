"""Repository for the drivers table."""

from f1_podium.db.repositories.base import TableRepository


class DriverRepository(TableRepository):
    """Read and write driver reference data."""

    table_name = "drivers"
