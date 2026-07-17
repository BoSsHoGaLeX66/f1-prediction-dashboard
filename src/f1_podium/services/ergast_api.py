"""Service wrapper for FastF1's Ergast API client."""

from typing import Any

import pandas as pd
from fastf1.ergast import Ergast


class FastF1ErgastService:
    """Encapsulate access to Ergast data through FastF1."""

    def __init__(self, ergast_client: Ergast | None = None) -> None:
        self._ergast_client = ergast_client or Ergast()

    def get_quali_data(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Fetch the latest qualifying data in the shape used by prediction flows."""
        response = self._ergast_client.get_qualifying_results(
            "current", "last", result_type="pandas"
        )

        qualifying_results = response.content[0]
        race_description = response.description

        qualifying_results["year"] = race_description["season"].values[0]
        qualifying_results["round"] = race_description["round"].values[0]
        qualifying_results.rename(columns={"position": "grid"}, inplace=True)

        return qualifying_results, race_description

    def get_race_data(self) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        """Fetch the latest race results in the previous Ergast JSON payload shape."""
        response = self._ergast_client.get_race_results(
            "current", "last", result_type="raw", auto_cast=False
        )

        if not response:
            return None, None

        race = response[0]
        return race, self._build_race_table(race)

    @staticmethod
    def _build_race_table(race: dict[str, Any]) -> dict[str, Any]:
        """Recreate the small RaceTable metadata object used by existing flows."""
        return {
            "season": race.get("season"),
            "round": race.get("round"),
            "Races": [race],
        }
