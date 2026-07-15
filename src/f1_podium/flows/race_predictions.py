from prefect import task, flow
from prefect.logging import get_run_logger
import pandas as pd
from prefect.cache_policies import NO_CACHE
from prefect.artifacts import create_table_artifact
from fastf1.ergast import Ergast
import numpy as np
import mlflow


# Robust imports that work both as a package and as a script
try:
    from f1_podium.datasets.builder import DatasetBuilder
    from f1_podium.db.connection import DatabaseConnection
    from f1_podium.db.repositories import (
        CircuitRepository,
        ConstructorRepository,
        DriverRepository,
        PredictionRepository,
        RaceRepository,
    )
    from f1_podium.features.engineer import FeatureEngineer
except ModuleNotFoundError:  # running as a script
    import sys
    from pathlib import Path

    sys.path.append(str(Path(__file__).resolve().parents[2]))  # add src
    from f1_podium.datasets.builder import DatasetBuilder
    from f1_podium.db.connection import DatabaseConnection
    from f1_podium.db.repositories import (
        CircuitRepository,
        ConstructorRepository,
        DriverRepository,
        PredictionRepository,
        RaceRepository,
    )
    from f1_podium.features.engineer import FeatureEngineer


@task
def make_predictions(data: pd.DataFrame):
    logger = get_run_logger()
    mlflow.set_tracking_uri("http://localhost:5000")
    model = mlflow.sklearn.load_model("models:/f1_model_prod@champion")

    preds = model.predict_proba(data[model.feature_names_in_])
    pred_vals = model.predict(data[model.feature_names_in_])

    preds = DatasetBuilder.build_prediction_probability_dataframe(preds)
    pred_vals = DatasetBuilder.build_prediction_value_dataframe(pred_vals)

    logger.info(preds)
    logger.info(pred_vals)

    data.reset_index(drop=True, inplace=True)

    pred_data = DatasetBuilder.build_prediction_output_dataframe(data, preds)
    logger.info(pred_data)

    create_table_artifact(
        key="predictions",
        table=pred_data.to_dict(orient="records"),
        description="the predicted labs of the data",
    )

    connection = DatabaseConnection.from_prefect_block_sync()
    PredictionRepository(connection).append(pred_data)


@task(cache_policy=NO_CACHE)
def load_data(round: int):
    connection = DatabaseConnection.from_prefect_block_sync()
    return RaceRepository(connection).get_prediction_columns()


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
    df_drivers = DatasetBuilder.append_reference_row(
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
    df_constructors = DatasetBuilder.append_reference_row(
        df_constructors,
        row[
            "constructorId",
            "constructorRef",
            "constructorName",
            "consctructorNationality",
        ],
    )

    return constuctor_id


@task
def clean_quali_data(df_quali: pd.DataFrame, df_desc: pd.DataFrame):
    connection = DatabaseConnection.from_prefect_block_sync()
    df_drivers = DriverRepository(connection).get_all()
    df_circuits = CircuitRepository(connection).get_all()
    df_constructors = ConstructorRepository(connection).get_all()

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
    df_results = FeatureEngineer.build(df_results, df_results_full)

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

    df_data = DatasetBuilder.combine_race_and_qualifying_data(data, df_quali)

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
