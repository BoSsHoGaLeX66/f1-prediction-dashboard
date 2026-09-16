"""Repository for the drivers table."""

from f1_podium.db.repositories.queryable import QueryableRepository


class DriverRepository(QueryableRepository):
    """Read and write driver reference data."""

    table_name = "drivers"
    reference_key = "driverId"
