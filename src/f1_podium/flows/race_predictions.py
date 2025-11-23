from prefect import task, flow
from prefect.logging import get_run_logger
import pandas as pd
from datetime import datetime
from prefect.cache_policies import NO_CACHE
from prefect.artifacts import create_table_artifact
from fastf1.ergast import Ergast
import numpy as np
import swifter
import mlflow


# Robust imports that work both as a package and as a script
try:
    from f1_podium.blocks.postgresql_conn import PostgresqlConnector
    from f1_podium.utils.data_utils import (
        top3_finishes,
        avg_finish_position_season,
        constructor_top_3,
        percent_top_3_at_circuit,
    )
except ModuleNotFoundError:  # running as a script
    import sys
    from pathlib import Path

    sys.path.append(str(Path(__file__).resolve().parents[1]))  # add src/f1_podium
    from blocks.postgresql_conn import PostgresqlConnector
    from utils.data_utils import (
        top3_finishes,
        avg_finish_position_season,
        constructor_top_3,
        percent_top_3_at_circuit,
    )


@task
def make_predictions(data: pd.DataFrame):
    logger = get_run_logger()
    mlflow.set_tracking_uri("http://localhost:5000")
    model = mlflow.sklearn.load_model("models:/f1_model_prod@champion")

    preds = model.predict_proba(data[model.feature_names_in_])
    pred_vals = model.predict(data[model.feature_names_in_])

    preds = pd.DataFrame(preds, columns=["no_podium", "podium"])
    pred_vals = pd.DataFrame(pred_vals, columns=["pred"])

    logger.info(preds)
    logger.info(pred_vals)

    data.reset_index(drop=True, inplace=True)

    pred_data = pd.concat(
        [data[["driverId", "constructorId", "year", "round"]], preds],
        axis=1,
    )
    logger.info(pred_data)

    create_table_artifact(
        key="predictions",
        table=pred_data.to_dict(orient="records"),
        description="the predicted labs of the data",
    )

    engine = PostgresqlConnector.load("postgresdb")

    pred_data.to_sql("predictions", engine, if_exists="append", index=False)


@task(cache_policy=NO_CACHE)
def load_data(round: int):
    postgres = PostgresqlConnector.load("postgresdb")
    engine = postgres.get_engine()
    data = pd.read_sql(
        "SELECT * FROM race_results",
        engine,
    )

    return data.loc[
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


@task
def get_quali_data():
    ergast = Ergast()
    resp = ergast.get_qualifying_results("current", "last", result_type="pandas")

    df_quali = resp.content[0]
    df_desc = resp.description

    df_quali["year"] = df_desc["season"].values[0]
    df_quali["round"] = df_desc["round"].values[0]
    df_quali.rename(columns={"position": "grid"}, inplace=True)

    return df_quali, df_desc


def get_circuit_id(circuitRef, df_circuits):
    exists = df_circuits.loc[df_circuits["circuitRef"] == circuitRef]
    exists = exists.reset_index(drop=True)
    if not exists.empty:
        return exists.at[0, "circuitId"]

    return df_circuits["circuitId"].max() + 1


def map_driver_id(row, df_drivers):
    driverRef = row.at["driverId"]
    exists = df_drivers.loc[df_drivers["driverRef"] == driverRef]
    exists = exists.reset_index(drop=True)
    if not exists.empty:
        return exists.at[0, "driverId"]
    driver_id = df_drivers["driverId"].max() + 1

    row.at["driverId"] = driver_id
    df_drivers = pd.concat(
        [
            df_drivers,
            row[
                "driverId",
                "driverNumber",
                "driverCode",
                "givenName",
                "familyName",
                "dateOfBirth",
                "driverNationality",
            ],
        ]
    )

    return driver_id


def map_constructor(row, df_constructors):
    constructorRef = row["constructorId"]
    exists = df_constructors.loc[df_constructors["constructorRef"] == constructorRef]
    exists = exists.reset_index(drop=True)
    if not exists.empty:
        return exists.at[0, "constructorId"]

    constuctor_id = df_constructors["constructorId"].max() + 1

    row.at["constructorId"] = constuctor_id
    df_constructors = pd.concat(
        [
            df_constructors,
            row[
                "constructorId",
                "constructorRef",
                "constructorName",
                "consctructorNationality",
            ],
        ]
    )

    return constuctor_id


@task
def clean_quali_data(df_quali: pd.DataFrame, df_desc: pd.DataFrame):
    engine = PostgresqlConnector.load("postgresdb").get_engine()
    with engine.begin() as conn:
        df_drivers = pd.read_sql("SELECT * FROM drivers", conn)
        df_circuits = pd.read_sql("SELECT * FROM circuits", conn)
        df_constructors = pd.read_sql("SELECT * FROM constructors", conn)

    circuit_id = get_circuit_id(df_desc["circuitId"].values[0], df_circuits)
    df_quali["circuitId"] = circuit_id
    df_quali["driverId"] = df_quali.apply(map_driver_id, axis=1, args=(df_drivers,))
    df_quali["constructorId"] = df_quali.apply(
        map_constructor, axis=1, args=(df_constructors,)
    )

    df_quali.drop(
        columns=[
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
        ],
        inplace=True,
    )
    df_quali["positionOrder"] = np.nan
    df_quali["statusId"] = np.nan

    return df_quali


@task
def create_pred_data(df_results: pd.DataFrame, df_results_full):
    logger = get_run_logger()
    # Convert positionOrder to a int and use it to get out target
    df_results.fillna(0, inplace=True)
    df_results["positionOrder"] = df_results["positionOrder"].astype(int)
    df_results["top_3"] = df_results["positionOrder"] < 4

    df_results_full.fillna(0, inplace=True)
    df_results_full["top_3"] = df_results_full["positionOrder"].astype(int)

    df_results["top3_driver_season_percentage"] = df_results.swifter.apply(
        top3_finishes, axis=1, args=(df_results,)
    )

    df_results["driver_avg_finish_pos_season"] = df_results.swifter.apply(
        avg_finish_position_season, axis=1, args=(df_results,)
    )

    df_results["Constructor_Top3_Percent"] = df_results.swifter.apply(
        constructor_top_3, axis=1, args=(df_results,)
    )

    df_results["Top_3_at_circuit"] = df_results.swifter.apply(
        percent_top_3_at_circuit, axis=1, args=(df_results_full,)
    )

    df_results.fillna(0)

    # Making moving averages for different time periods
    drivers = df_results["driverId"].unique()

    df_results_full.sort_values(["year", "round"], inplace=True)
    df_results.top_3 = df_results.top_3.astype(int)
    df_results_full.top_3 = df_results.top_3.astype(int)

    for driver in drivers:
        df_results_full.loc[
            df_results_full["driverId"] == driver, "Finish_Pos_Last_Race"
        ] = df_results_full.loc[
            df_results_full["driverId"] == driver
        ].positionOrder.shift(1)

        df_results_full.loc[
            df_results_full["driverId"] == driver, "Top_3_Last_Race"
        ] = df_results_full.loc[df_results_full["driverId"] == driver].top_3.shift(1)

        df_results_full.loc[df_results_full["driverId"] == driver, "Finish_Mean_3"] = (
            df_results_full.loc[df_results_full["driverId"] == driver][
                "Finish_Pos_Last_Race"
            ]
            .rolling(3)
            .mean()
        )
        df_results_full.loc[df_results_full["driverId"] == driver, "Finish_Mean_5"] = (
            df_results_full.loc[df_results_full["driverId"] == driver][
                "Finish_Pos_Last_Race"
            ]
            .rolling(5)
            .mean()
        )
        df_results_full.loc[df_results_full["driverId"] == driver, "Finish_Mean_10"] = (
            df_results_full.loc[df_results_full["driverId"] == driver][
                "Finish_Pos_Last_Race"
            ]
            .rolling(10)
            .mean()
        )

    max_round = pd.DataFrame(
        df_results.groupby(["year"], as_index=False)["round"].max(),
        columns=["year", "round"],
    )
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

    last_race_stats = last_race_stats.sort_values(["year", "round"], ascending=True)

    df_results = last_race_stats.copy()
    df_results.fillna(0, inplace=True)

    df_results.top_3.astype(int)

    df_results_full.drop_duplicates(subset=["driverId", "year", "round"], inplace=True)

    df_results = df_results.merge(
        df_results_full,
        how="left",
        left_on=["driverId", "year", "round"],
        right_on=["driverId", "year", "round"],
        suffixes=("", "_extra"),
    )

    df_results.drop(
        columns=[x for x in df_results.columns if "_extra" in x], inplace=True
    )

    df_results.fillna(0, inplace=True)
    df_results.drop_duplicates(subset=["driverId", "year", "round"], inplace=True)

    create_table_artifact(
        key="f1-pred-data",
        table=df_results.to_dict(orient="records"),
        description="data to be used for prediction",
    )

    logger.info(f"df_results columns: {df_results.columns}")
    logger.info(f"Table: {df_results.head()}")

    return df_results


@flow(name="run_pred")
async def run_pred(round: int):
    data = load_data(round)
    df_quali, df_desc = get_quali_data()

    df_quali = clean_quali_data(df_quali, df_desc)

    df_data = pd.concat([data, df_quali])

    max_year = df_data["year"].max()
    df_proc_data = create_pred_data(
        df_data.loc[(df_data["year"] == max_year) | (df_data["year"] == max_year - 1)],
        df_data,
    )

    make_predictions(
        df_proc_data.loc[
            (df_proc_data["round"] == round) & (df_proc_data["year"] == max_year)
        ]
    )


if __name__ == "__main__":
    run_pred.serve()
