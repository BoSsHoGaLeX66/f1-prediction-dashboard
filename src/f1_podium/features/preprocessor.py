"""Preprocessing helpers for model input data."""

import numpy as np
import pandas as pd

from f1_podium.datasets.builder import DatasetBuilder


class Preprocessor:
    """Preprocess raw flow data into the schema expected by feature engineering."""

    QUALIFYING_DROP_COLUMNS = [
        "Q1",
        "Q2",
        "Q3",
        "driverNumber",
        "driverCode",
        "driverUrl",
        "givenName",
        "familyName",
        "dateOfBirth",
        "driverNationality",
        "constructorUrl",
        "constructorName",
        "constructorNationality",
    ]

    def __init__(
        self,
        drivers: pd.DataFrame,
        circuits: pd.DataFrame,
        constructors: pd.DataFrame,
    ) -> None:
        self._drivers = drivers
        self._circuits = circuits
        self._constructors = constructors

    def clean_quali_data(
        self, qualifying_data: pd.DataFrame, race_description: pd.DataFrame
    ) -> pd.DataFrame:
        """Clean qualifying rows so they can be combined with race result rows."""
        circuit_id = self.get_circuit_id(race_description["circuitId"].values[0])
        qualifying_data["circuitId"] = circuit_id
        qualifying_data["driverId"] = qualifying_data.apply(
            self.map_driver_id, axis=1
        )
        qualifying_data["constructorId"] = qualifying_data.apply(
            self.map_constructor_id, axis=1
        )

        qualifying_data.drop(columns=self.QUALIFYING_DROP_COLUMNS, inplace=True)
        qualifying_data["positionOrder"] = np.nan
        qualifying_data["statusId"] = np.nan

        return qualifying_data

    def get_circuit_id(self, circuit_ref: str) -> int:
        """Return the local circuit id for an Ergast circuit reference."""
        exists = self._circuits.loc[self._circuits["circuitRef"] == circuit_ref]
        exists = exists.reset_index(drop=True)
        if not exists.empty:
            return exists.at[0, "circuitId"]

        return self._circuits["circuitId"].max() + 1

    def map_driver_id(self, row: pd.Series) -> int:
        """Map an Ergast driver reference to a local driver id."""
        driver_ref = row.at["driverId"]
        exists = self._drivers.loc[self._drivers["driverRef"] == driver_ref]
        exists = exists.reset_index(drop=True)
        if not exists.empty:
            return exists.at[0, "driverId"]
        driver_id = self._drivers["driverId"].max() + 1

        row.at["driverId"] = driver_id
        DatasetBuilder.append_reference_row(
            self._drivers,
            row[
                "driverId",
                "driverNumber",
                "driverCode",
                "givenName",
                "familyName",
                "dateOfBirth",
                "driverNationality",
            ],
        )

        return driver_id

    def map_constructor_id(self, row: pd.Series) -> int:
        """Map an Ergast constructor reference to a local constructor id."""
        constructor_ref = row["constructorId"]
        exists = self._constructors.loc[
            self._constructors["constructorRef"] == constructor_ref
        ]
        exists = exists.reset_index(drop=True)
        if not exists.empty:
            return exists.at[0, "constructorId"]

        constuctor_id = self._constructors["constructorId"].max() + 1

        row.at["constructorId"] = constuctor_id
        DatasetBuilder.append_reference_row(
            self._constructors,
            row[
                "constructorId",
                "constructorRef",
                "constructorName",
                "consctructorNationality",
            ],
        )

        return constuctor_id
