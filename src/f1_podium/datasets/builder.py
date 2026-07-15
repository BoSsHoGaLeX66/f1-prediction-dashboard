"""DataFrame builders used by ingestion and prediction flows."""

import pandas as pd


class DatasetBuilder:
    """Build schema-aligned DataFrames for pipeline tasks."""

    @staticmethod
    def dataframe_or_none(rows: list, columns: list[str]) -> pd.DataFrame | None:
        """Create a DataFrame from rows, or return None when no rows are staged."""
        if not rows:
            return None

        return pd.DataFrame(rows, columns=columns)

    @staticmethod
    def get_result_time_fields(result: dict, status_text: str) -> tuple[str, str]:
        """Extract race time and milliseconds for completed or lapped results."""
        time_obj = result.get("Time")
        if status_text not in ["Finished", "Lapped"] or not time_obj:
            return "", ""

        return time_obj.get("time", ""), time_obj.get("millis", "")

    @staticmethod
    def get_fastest_lap_fields(result: dict) -> tuple[str, str, str]:
        """Extract fastest lap number, rank, and lap time from a result payload."""
        fastest_lap_data = result.get("FastestLap") or {}
        fastest_lap_time = (fastest_lap_data.get("Time") or {}).get("time", "")
        return (
            fastest_lap_data.get("lap", ""),
            fastest_lap_data.get("rank", ""),
            fastest_lap_time,
        )

    @staticmethod
    def build_race_result_row(
        result: dict,
        driver_id: int,
        constructor_id: int,
        status_id: int,
        year: int,
        round_num: int,
        circuit_id: int,
    ) -> list:
        """Build one schema-ordered race result row for insertion."""
        status_text = result["status"]
        race_time, millis = DatasetBuilder.get_result_time_fields(result, status_text)
        fastest_lap, fastest_lap_rank, fastest_lap_time = (
            DatasetBuilder.get_fastest_lap_fields(result)
        )

        return [
            driver_id,
            constructor_id,
            result.get("number"),
            result.get("grid"),
            result.get("position"),
            result.get("positionText"),
            result.get("position"),
            result.get("points"),
            result.get("laps"),
            race_time,
            millis,
            fastest_lap,
            fastest_lap_rank,
            fastest_lap_time,
            0,
            status_id,
            year,
            round_num,
            circuit_id,
        ]

    @staticmethod
    def build_insert_dataframes(
        data_table: list,
        driver_table: list,
        constructor_table: list,
        status_table: list,
    ) -> tuple[
        pd.DataFrame, pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None
    ]:
        """Convert staged race and reference rows into schema-aligned DataFrames."""
        drivers_df = DatasetBuilder.dataframe_or_none(
            driver_table,
            [
                "driverId",
                "driverRef",
                "number",
                "code",
                "forename",
                "surname",
                "dob",
                "nationality",
            ],
        )
        constructors_df = DatasetBuilder.dataframe_or_none(
            constructor_table,
            ["constructorId", "constructorRef", "name", "nationality"],
        )
        statuses_df = DatasetBuilder.dataframe_or_none(
            status_table, ["statusId", "status"]
        )

        results_columns = [
            "driverId",
            "constructorId",
            "number",
            "grid",
            "position",
            "positionText",
            "positionOrder",
            "points",
            "laps",
            "time",
            "milliseconds",
            "fastestLap",
            "rank",
            "fastestLapTime",
            "fastestLapSpeed",
            "statusId",
            "year",
            "round",
            "circuitId",
        ]
        results_df = pd.DataFrame(data_table, columns=results_columns)

        return results_df, drivers_df, constructors_df, statuses_df

    @staticmethod
    def build_prediction_probability_dataframe(predictions) -> pd.DataFrame:
        """Build a probability DataFrame from model prediction probabilities."""
        return pd.DataFrame(predictions, columns=["no_podium", "podium"])

    @staticmethod
    def build_prediction_value_dataframe(prediction_values) -> pd.DataFrame:
        """Build a prediction label DataFrame from model predictions."""
        return pd.DataFrame(prediction_values, columns=["pred"])

    @staticmethod
    def build_prediction_output_dataframe(
        data: pd.DataFrame, predictions: pd.DataFrame
    ) -> pd.DataFrame:
        """Combine prediction inputs and probabilities into persisted output rows."""
        return pd.concat(
            [data[["driverId", "constructorId", "year", "round"]], predictions],
            axis=1,
        )

    @staticmethod
    def append_reference_row(data: pd.DataFrame, row) -> pd.DataFrame:
        """Append a staged reference row to an existing reference DataFrame."""
        return pd.concat([data, row])

    @staticmethod
    def build_max_round_dataframe(df_results: pd.DataFrame) -> pd.DataFrame:
        """Build a season-level DataFrame containing each year's maximum round."""
        return pd.DataFrame(
            df_results.groupby(["year"], as_index=False)["round"].max(),
            columns=["year", "round"],
        )

    @staticmethod
    def combine_race_and_qualifying_data(
        race_data: pd.DataFrame, qualifying_data: pd.DataFrame
    ) -> pd.DataFrame:
        """Combine historical race rows with current qualifying rows."""
        return pd.concat([race_data, qualifying_data])
