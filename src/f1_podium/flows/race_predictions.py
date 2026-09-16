from prefect import task, flow
from prefect.logging import get_run_logger
import pandas as pd
from prefect.cache_policies import NO_CACHE
from prefect.artifacts import create_table_artifact
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
    from f1_podium.features.preprocessor import Preprocessor
    from f1_podium.services import FastF1Service
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
    from f1_podium.features.preprocessor import Preprocessor
    from f1_podium.services import FastF1Service


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
    df_quali, df_desc = FastF1Service().get_quali_data()

    connection = DatabaseConnection.from_prefect_block_sync()
    preprocessor = Preprocessor(
        drivers=DriverRepository(connection).get_all(),
        circuits=CircuitRepository(connection).get_all(),
        constructors=ConstructorRepository(connection).get_all(),
    )
    df_quali = preprocessor.clean_quali_data(df_quali, df_desc)

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
