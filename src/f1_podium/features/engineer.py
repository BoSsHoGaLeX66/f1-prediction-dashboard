"""Feature engineering for F1 podium prediction datasets."""

import pandas as pd
import swifter  # noqa: F401

from f1_podium.datasets.builder import DatasetBuilder


class FeatureEngineer:
    """Build model-ready feature DataFrames from race result data."""

    @staticmethod
    def build(df_results: pd.DataFrame, df_results_full: pd.DataFrame) -> pd.DataFrame:
        """Build the full engineered feature DataFrame for model prediction."""
        FeatureEngineer.add_target_columns(df_results, df_results_full)
        FeatureEngineer.add_season_history_features(df_results, df_results_full)
        FeatureEngineer.add_driver_rolling_features(df_results, df_results_full)

        df_results = FeatureEngineer.add_previous_season_features(df_results)
        df_results = FeatureEngineer.merge_rolling_features(df_results, df_results_full)
        return FeatureEngineer.finalize_features(df_results)

    @staticmethod
    def add_target_columns(
        df_results: pd.DataFrame, df_results_full: pd.DataFrame
    ) -> None:
        """Add target and normalized position columns required by later features."""
        df_results.fillna(0, inplace=True)
        df_results["positionOrder"] = df_results["positionOrder"].astype(int)
        df_results["top_3"] = df_results["positionOrder"] < 4

        df_results_full.fillna(0, inplace=True)
        df_results_full["top_3"] = df_results_full["positionOrder"].astype(int)

    @staticmethod
    def add_season_history_features(
        df_results: pd.DataFrame, df_results_full: pd.DataFrame
    ) -> None:
        """Add driver, constructor, and circuit historical performance features."""
        df_results["top3_driver_season_percentage"] = df_results.swifter.apply(
            FeatureEngineer.top3_finishes, axis=1, args=(df_results,)
        )

        df_results["driver_avg_finish_pos_season"] = df_results.swifter.apply(
            FeatureEngineer.avg_finish_position_season, axis=1, args=(df_results,)
        )

        df_results["Constructor_Top3_Percent"] = df_results.swifter.apply(
            FeatureEngineer.constructor_top_3, axis=1, args=(df_results,)
        )

        df_results["Top_3_at_circuit"] = df_results.swifter.apply(
            FeatureEngineer.percent_top_3_at_circuit, axis=1, args=(df_results_full,)
        )

        df_results.fillna(0)

    @staticmethod
    def top3_finishes(row: pd.Series, df: pd.DataFrame):
        """Calculate a driver's prior top-three finish rate in the same season."""
        top_3_count = df[
            (df.year == row.year)
            & (df["round"] < row["round"])
            & (df.driverId == row.driverId)
        ].top_3.sum()
        top_3_freq = (
            top_3_count
            / df[
                (df.year == row.year)
                & (df.driverId == row.driverId)
                & (df["round"] < row["round"])
            ].driverId.count()
        )

        return top_3_freq

    @staticmethod
    def avg_finish_position_season(row: pd.Series, df: pd.DataFrame):
        """Calculate a driver's average prior finish position in the same season."""
        return df.loc[
            (df.driverId == row.driverId)
            & (df.year == row.year)
            & (df["round"] < row["round"]),
            "positionOrder",
        ].mean()

    @staticmethod
    def constructor_top_3(row: pd.Series, df: pd.DataFrame):
        """Calculate a constructor's prior top-three finish rate in the season."""
        top_3_count = df[
            (df["year"] == row.year)
            & (df.constructorId == row.constructorId)
            & (df["round"] < row["round"])
        ].top_3.sum()
        top_3_freq = (
            top_3_count
            / df[
                (df.year == row.year)
                & (df.constructorId == row.constructorId)
                & (df["round"] < row["round"])
            ].driverId.count()
        )
        return top_3_freq

    @staticmethod
    def percent_top_3_at_circuit(row: pd.Series, df: pd.DataFrame):
        """Calculate a driver's historical top-three rate at the same circuit."""
        return (
            df[
                (df.circuitId == row.circuitId)
                & (df.driverId == row.driverId)
                & (df["year"] < row["year"])
            ].top_3.sum()
            / df[
                (df.circuitId == row.circuitId) & (df.driverId == row.driverId)
            ].circuitId.count()
        )

    @staticmethod
    def add_driver_rolling_features(
        df_results: pd.DataFrame, df_results_full: pd.DataFrame
    ) -> None:
        """Add lagged finish and rolling finish statistics for each driver."""
        drivers = df_results["driverId"].unique()

        df_results_full.sort_values(["year", "round"], inplace=True)
        df_results.top_3 = df_results.top_3.astype(int)
        df_results_full.top_3 = df_results.top_3.astype(int)

        for driver in drivers:
            driver_mask = df_results_full["driverId"] == driver

            df_results_full.loc[driver_mask, "Finish_Pos_Last_Race"] = (
                df_results_full.loc[driver_mask].positionOrder.shift(1)
            )
            df_results_full.loc[driver_mask, "Top_3_Last_Race"] = df_results_full.loc[
                driver_mask
            ].top_3.shift(1)
            df_results_full.loc[driver_mask, "Finish_Mean_3"] = (
                df_results_full.loc[driver_mask]["Finish_Pos_Last_Race"]
                .rolling(3)
                .mean()
            )
            df_results_full.loc[driver_mask, "Finish_Mean_5"] = (
                df_results_full.loc[driver_mask]["Finish_Pos_Last_Race"]
                .rolling(5)
                .mean()
            )
            df_results_full.loc[driver_mask, "Finish_Mean_10"] = (
                df_results_full.loc[driver_mask]["Finish_Pos_Last_Race"]
                .rolling(10)
                .mean()
            )

    @staticmethod
    def add_previous_season_features(df_results: pd.DataFrame) -> pd.DataFrame:
        """Attach previous-season final-round feature values to each driver row."""
        max_round = DatasetBuilder.build_max_round_dataframe(df_results)
        max_round["year"] = max_round["year"] + 1

        last_race_stats = df_results.merge(
            max_round, on=["year"], how="left", suffixes=["", "_max_last_szn"]
        )

        last_race_stats.dropna(inplace=True)
        lag_df = df_results.copy()
        lag_df["year"] = lag_df["year"] + 1
        last_race_stats = last_race_stats.merge(
            lag_df[
                [
                    "top3_driver_season_percentage",
                    "driver_avg_finish_pos_season",
                    "Constructor_Top3_Percent",
                    "year",
                    "round",
                    "driverId",
                ]
            ],
            left_on=["year", "round_max_last_szn", "driverId"],
            right_on=["year", "round", "driverId"],
            how="left",
            suffixes=("", "_lag"),
        )

        return last_race_stats.sort_values(["year", "round"], ascending=True)

    @staticmethod
    def merge_rolling_features(
        df_results: pd.DataFrame, df_results_full: pd.DataFrame
    ) -> pd.DataFrame:
        """Merge lagged rolling features back onto the prediction feature rows."""
        df_results = df_results.copy()
        df_results.fillna(0, inplace=True)
        df_results.top_3.astype(int)

        df_results_full.drop_duplicates(
            subset=["driverId", "year", "round"], inplace=True
        )

        df_results = df_results.merge(
            df_results_full,
            how="left",
            left_on=["driverId", "year", "round"],
            right_on=["driverId", "year", "round"],
            suffixes=("", "_extra"),
        )

        df_results.drop(
            columns=[column for column in df_results.columns if "_extra" in column],
            inplace=True,
        )

        return df_results

    @staticmethod
    def finalize_features(df_results: pd.DataFrame) -> pd.DataFrame:
        """Fill missing values and remove duplicate driver-round feature rows."""
        df_results.fillna(0, inplace=True)
        df_results.drop_duplicates(subset=["driverId", "year", "round"], inplace=True)
        return df_results
