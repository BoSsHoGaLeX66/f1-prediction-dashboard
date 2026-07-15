"""Repository for the processed_race_data table."""

from f1_podium.db.repositories.base import TableRepository


class ProcessedDataRepository(TableRepository):
    """Read and write processed race feature data."""

    table_name = "processed_race_data"
