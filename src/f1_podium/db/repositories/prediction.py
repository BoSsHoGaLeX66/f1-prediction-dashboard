"""Repository for the predictions table."""

from f1_podium.db.repositories.queryable import QueryableRepository


class PredictionRepository(QueryableRepository):
    """Read and write model prediction rows."""

    table_name = "predictions"
    circuit_from_race = True
