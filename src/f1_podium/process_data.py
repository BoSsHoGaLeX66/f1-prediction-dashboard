import pandas as pd
from prefect import task, flow
from prefect.logging import get_run_logger
import postgresql_conn
import requests
from sqlalchemy import text


def check_circuit(circuits: pd.DataFrame, max_circuit: int, race: dict) -> int:
    circuit = race["Circuit"]["circuitId"]
    try:
        circuitId = circuits.loc[circuits["circuitRef"] == circuit, "circuitId"].values[
            0
        ]
    except KeyError:
        circuitId = max_circuit
        max_circuit += 1
    return circuitId


def load_sql_data(engine) -> None:
    circuits = pd.read_sql("SELECT * FROM circuits", engine)
    drivers = pd.read_sql("SELECT * FROM drivers", engine)
    constructors = pd.read_sql("SELECT * FROM constructors", engine)
    statuses = pd.read_sql("SELECT *FROM status", engine)
    return circuits, constructors, drivers, statuses


@task
def get_latest_race():
    logger = get_run_logger()

    req = requests.get("https://api.jolpi.ca/ergast/f1/current/last/results/")
    data = req.json()
    engine = postgresql_conn.PostgresqlConnector.load("postgresdb")

    circuits = pd.read_sql("SELECT * FROM circuits", engine)
    drivers = pd.read_sql("SELECT * FROM drivers", engine)
    constructors = pd.read_sql("SELECT * FROM constructors", engine)
    statuses = pd.read_sql("SELECT *FROM status", engine)

    max_circuit = circuits["circuitId"].max() + 1
    max_driverId = drivers["driverId"].max() + 1
    max_constructorId = constructors["constructorId"].max() + 1
    max_statusId = statuses["statusId"].max() + 1

    round_num = data["MRData"]["RaceTable"]["round"]
    year = data["MRData"]["RaceTable"]["season"]

    with engine.connect() as conn:
        result = conn.execute(f"SELECT MAX(round) FROM race_data WHERE year = {year}")

    if result == round_num:
        logger.info(f"INFO result for race ({year}, {round}) already in database")
        return 0

    race = data["MRData"]["RaceTable"]["Races"][0]["Results"]

    circuitId = check_circuit(circuits, max_circuit, race)
    data_table = []

    for result in race["Results"]:
        driver = result["Driver"]["driverId"]
        if not drivers.loc[drivers["driverRef"] == driver, "driverId"].empty:
            driverId = drivers.loc[drivers["driverRef"] == driver, "driverId"].values[0]
        else:
            driverId = max_driverId
            max_driverId += 1

        constructor = result["Constructor"]["constructorId"]
        if not constructors.loc[
            constructors["constructorRef"] == constructor, "constructorId"
        ].empty:
            constructorId = constructors.loc[
                constructors["constructorRef"] == constructor, "constructorId"
            ].values[0]
        else:
            constructorId = max_constructorId
            max_constructorId += 1

        status = result["status"]
        if not statuses.loc[statuses["status"] == status, "statusId"].empty:
            statusId = statuses.loc[statuses["status"] == status, "statusId"].values[0]
        else:
            statusId = max_statusId
            max_statusId += 1

        if not status in ["Finished", "Lapped"]:
            race_time = ""
            millis = ""
        else:
            race_time = result["Time"]["time"]
            millis = result["Time"]["millis"]

        try:
            fastest_lap = result["FastestLap"]["lap"]
            fastest_lap_rank = result["FastestLap"]["rank"]
            fastest_lap_time = result["FastestLap"]["Time"]["time"]
        except:
            fastest_lap = ""
            fastest_lap_rank = ""
            fastest_lap_time = ""

        row = [
            driverId,
            constructorId,
            result["number"],
            result["grid"],
            result["position"],
            result["positionText"],
            result["position"],
            result["points"],
            result["laps"],
            race_time,
            millis,
            fastest_lap,
            fastest_lap_rank,
            fastest_lap_time,
            0,
            statusId,
            year,
            round_num,
            circuitId,
        ]
        data_table.append(row)
