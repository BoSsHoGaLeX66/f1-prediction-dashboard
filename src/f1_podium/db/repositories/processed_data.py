"""Repository for the processed_race_data table."""

from f1_podium.db.repositories.queryable import QueryableRepository


class ProcessedDataRepository(QueryableRepository):
    """Read and write processed race feature data."""

    table_name = "processed_race_data"
