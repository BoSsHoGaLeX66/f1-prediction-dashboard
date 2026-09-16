"""Repository for the statuses table."""

from f1_podium.db.repositories.queryable import QueryableRepository


class StatusRepository(QueryableRepository):
    """Read and write race status reference data."""

    table_name = "statuses"
    reference_key = "statusId"
