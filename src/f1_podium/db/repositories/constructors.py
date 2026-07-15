"""Repository for the constructors table."""

from f1_podium.db.repositories.base import TableRepository


class ConstructorRepository(TableRepository):
    """Read and write constructor reference data."""

    table_name = "constructors"
