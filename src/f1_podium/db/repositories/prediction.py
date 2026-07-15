"""Repository for the predictions table."""

from f1_podium.db.repositories.base import TableRepository


class PredictionRepository(TableRepository):
    """Read and write model prediction rows."""

    table_name = "predictions"
