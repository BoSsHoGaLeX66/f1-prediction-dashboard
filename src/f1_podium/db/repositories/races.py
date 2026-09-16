"""Repository for the race_results table."""

import pandas as pd
from sqlalchemy import text

from f1_podium.db.repositories.queryable import QueryableRepository


class RaceRepository(QueryableRepository):
    """Read and write race result rows."""

    table_name = "race_results"

    def latest_round_for_year(self, year: int) -> int | None:
        """Return the highest stored round for a season, if any exists."""
        with self.connection.engine.connect() as conn:
            return conn.execute(
                text("SELECT MAX(round) FROM race_results WHERE year = :year"),
                {"year": year},
            ).scalar()

    def has_round(self, year: int, round_num: int) -> bool:
        """Return whether the table already contains at least the target round."""
        latest_round = self.latest_round_for_year(year)
        return latest_round is not None and int(latest_round) >= round_num

    def get_prediction_columns(self) -> pd.DataFrame:
        """Return race result columns required for prediction feature creation."""
        return self.get_all().loc[
            :,
            [
                "driverId",
                "constructorId",
                "grid",
                "positionOrder",
                "statusId",
                "year",
                "round",
                "circuitId",
            ],
        ]
