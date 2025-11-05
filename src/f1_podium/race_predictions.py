from prefect import task, flow
import pandas as pd
import postgresql_conn
from datetime import datetime
from prefect.cache_policies import NO_CACHE


@task
def make_predictions(data: pd.DataFrame):
    print("Scheduled")


@task(cache_policy=NO_CACHE)
async def load_data(round: int):
    year = datetime.now().year
    postgres = postgresql_conn.PostgresqlConnector.load("postgresdb")
    engine = postgres.get_engine()
    data = pd.read_sql(
        f"SELECT * FROM processed_race_data WHERE year = {year} AND round = {round}",
        engine,
    )

    return data


@flow(name="run_pred")
async def run_pred(round: int):
    data = load_data(round)
    make_predictions(data)


if __name__ == "__main__":
    run_pred.serve()
