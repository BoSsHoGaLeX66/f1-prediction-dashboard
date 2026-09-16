"""Shared, parameterized lookups for repository-owned tables."""

import pandas as pd
from sqlalchemy import text

from f1_podium.db.repositories.base import TableRepository


class QueryableRepository(TableRepository):
    """Select full rows using local numeric IDs and exact season matches."""

    reference_key: str | None = None
    circuit_from_race = False

    def get_by_driver(self, driver_id: int) -> pd.DataFrame:
        """Return rows for a local driver ID."""
        return self.get_filtered(driver_id=driver_id)

    def get_by_circuit(self, circuit_id: int) -> pd.DataFrame:
        """Return rows for a local circuit (track) ID."""
        return self.get_filtered(circuit_id=circuit_id)

    def get_by_year(self, year: int) -> pd.DataFrame:
        """Return rows for a season."""
        return self.get_filtered(year=year)

    def get_by_team(self, team_id: int) -> pd.DataFrame:
        """Return rows for a local constructor (team) ID."""
        return self.get_filtered(team_id=team_id)

    def get_by_driver_and_year(self, driver_id: int, year: int) -> pd.DataFrame:
        """Return rows for a driver in a season."""
        return self.get_filtered(driver_id=driver_id, year=year)

    def get_by_driver_and_circuit(
        self, driver_id: int, circuit_id: int
    ) -> pd.DataFrame:
        """Return rows for a driver at a circuit."""
        return self.get_filtered(driver_id=driver_id, circuit_id=circuit_id)

    def get_filtered(
        self,
        *,
        driver_id: int | None = None,
        circuit_id: int | None = None,
        year: int | None = None,
        team_id: int | None = None,
    ) -> pd.DataFrame:
        """Return rows matching every supplied filter; None omits a filter.

        Team IDs are constructorId values. No filters returns all rows.
        Unknown values return an empty DataFrame with the table's columns.
        Reference tables use race participation for nonlocal filters, with
        every condition matching the same race result. Each stored reference
        row is returned once, even if several races match.
        Prediction circuits are resolved through stored race results by year
        and round; predictions without those results cannot match a circuit.
        Result ordering is unspecified.
        """
        filters = {
            "driverId": driver_id,
            "circuitId": circuit_id,
            "year": year,
            "constructorId": team_id,
        }
        params = {key: value for key, value in filters.items() if value is not None}
        local_keys = set(params) if self.reference_key is None else {self.reference_key}
        if self.circuit_from_race:
            local_keys.discard("circuitId")
        conditions = [f't."{key}" = :{key}' for key in params if key in local_keys]
        related_keys = [key for key in params if key not in local_keys]
        if related_keys:
            if self.reference_key is not None:
                race_conditions = [
                    f'r."{self.reference_key}" = t."{self.reference_key}"'
                ]
            else:
                race_conditions = ['r."year" = t."year"', 'r."round" = t."round"']
            race_conditions.extend(f'r."{key}" = :{key}' for key in related_keys)
            conditions.append(
                'EXISTS (SELECT 1 FROM "race_results" AS r WHERE '
                + " AND ".join(race_conditions)
                + ")"
            )
        query = f'SELECT t.* FROM "{self.table_name}" AS t'
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        return self.connection.read_sql(text(query), params=params)
