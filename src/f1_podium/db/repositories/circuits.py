"""Repository for the circuits table."""

from f1_podium.db.repositories.queryable import QueryableRepository


class CircuitRepository(QueryableRepository):
    """Read circuit reference data."""

    table_name = "circuits"
    reference_key = "circuitId"
