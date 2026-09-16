"""Repository for the constructors table."""

from f1_podium.db.repositories.queryable import QueryableRepository


class ConstructorRepository(QueryableRepository):
    """Read and write constructor reference data."""

    table_name = "constructors"
    reference_key = "constructorId"
