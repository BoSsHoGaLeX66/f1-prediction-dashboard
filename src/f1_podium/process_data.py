"""Data ingestion utilities for F1 race results.

This module contains Prefect tasks and helpers to fetch the latest race
results from the Ergast API and map them to local database identifiers.
"""

import pandas as pd
from prefect import task, flow
from prefect.logging import get_run_logger
import postgresql_conn
import requests
from sqlalchemy import text


def check_circuit(circuits: pd.DataFrame, max_circuit: int, race: dict) -> int:
    """Return the local circuitId for the given race.

    Looks up the circuit by its reference from the provided ``circuits`` table.
    If not found, returns the next available circuit id (``max_circuit``).
    """
    circuit = race["Circuit"]["circuitId"]
    try:
        circuitId = circuits.loc[circuits["circuitRef"] == circuit, "circuitId"].values[
            0
        ]
    except Exception:
        circuitId = max_circuit
    return int(circuitId)


def load_sql_data(engine):
    """Load reference tables required to map race data to local ids."""
    circuits = pd.read_sql("SELECT * FROM circuits", engine)
    drivers = pd.read_sql("SELECT * FROM drivers", engine)
    constructors = pd.read_sql("SELECT * FROM constructors", engine)
    statuses = pd.read_sql("SELECT * FROM status", engine)
    return circuits, constructors, drivers, statuses


@task
def get_latest_race() -> int:
    """Fetch the latest race results and prepare rows for insertion.

    - Pulls the most recent race results from the Ergast API.
    - Loads local reference tables to map drivers, constructors, circuits, and status.
    - Skips work if the latest round for the current season already exists.

    Returns the count of prepared rows (0 if up to date).
    """
    logger = get_run_logger()

    # Fetch latest results from Ergast (via Jolpica proxy)
    resp = requests.get(
        "https://api.jolpi.ca/ergast/f1/current/last/results/", timeout=15
    )
    resp.raise_for_status()
    payload = resp.json()

    race_table = payload.get("MRData", {}).get("RaceTable", {})
    races = race_table.get("Races", [])
    if not races:
        logger.warning("No races found in API response; nothing to do")
        return 0

    race = races[0]
    # Prefer values from the race object; fall back to RaceTable
    year = int(race.get("season") or race_table.get("season"))
    round_num = int(race.get("round") or race_table.get("round"))

    # Resolve DB engine from Prefect Block
    postgres_block = postgresql_conn.PostgresqlConnector.load("postgresdb")
    engine = postgres_block.get_engine()

    circuits, constructors, drivers, statuses = load_sql_data(engine)

    def next_id(df: pd.DataFrame, col: str) -> int:
        """Return the next integer id for a non-empty table else 1."""
        return int(df[col].max()) + 1 if not df.empty else 1

    max_circuit = next_id(circuits, "circuitId")
    max_driver_id = next_id(drivers, "driverId")
    max_constructor_id = next_id(constructors, "constructorId")
    max_status_id = next_id(statuses, "statusId")

    # Skip if this round already exists for the season
    with engine.connect() as conn:
        existing_round = conn.execute(
            text("SELECT MAX(round) FROM race_data WHERE year = :year"),
            {"year": year},
        ).scalar()

    if existing_round is not None and int(existing_round) >= round_num:
        logger.info(
            f"Latest results already present for {year} round {round_num}; skipping"
        )
        return 0

    circuit_id = check_circuit(circuits, max_circuit, race)
    data_table = []  # rows for race_data
    driver_table = []  # new driver rows (only when not present)
    constructor_table = []  # new constructor rows (only when not present)
    status_table = []  # new status rows (only when not present)

    for res in race.get("Results", []):
        # Driver mapping
        driver_ref = res["Driver"]["driverId"]
        if not drivers.loc[drivers["driverRef"] == driver_ref, "driverId"].empty:
            driver_id = int(
                drivers.loc[drivers["driverRef"] == driver_ref, "driverId"].values[0]
            )
        else:
            driver = res["Driver"]
            driver_id = max_driver_id
            driver_table.append(
                [
                    driver_id,
                    driver_ref,
                    res["number"],
                    driver["code"],
                    driver["givenName"],
                    driver["familyName"],
                    driver["dateOfBirth"],
                    driver["nationality"],
                ]
            )
            max_driver_id += 1

        # Constructor mapping
        constructor_ref = res["Constructor"]["constructorId"]
        if not constructors.loc[
            constructors["constructorRef"] == constructor_ref, "constructorId"
        ].empty:
            constructor_id = int(
                constructors.loc[
                    constructors["constructorRef"] == constructor_ref, "constructorId"
                ].values[0]
            )
        else:
            constructor = res["Constructor"]
            constructor_id = max_constructor_id
            constructor_table.append(
                [
                    constructor_id,
                    constructor_ref,
                    constructor["name"],
                    constructor["nationality"],
                ]
            )
            max_constructor_id += 1

        # Status mapping
        status_text = res["status"]
        if not statuses.loc[statuses["status"] == status_text, "statusId"].empty:
            status_id = int(
                statuses.loc[statuses["status"] == status_text, "statusId"].values[0]
            )
        else:
            status_id = max_status_id
            status_table.append([status_id, status_text])
            max_status_id += 1

        # Time fields
        time_obj = res.get("Time")
        if status_text not in ["Finished", "Lapped"] or not time_obj:
            race_time = ""
            millis = ""
        else:
            race_time = time_obj.get("time", "")
            millis = time_obj.get("millis", "")

        # Fastest lap fields
        fl = res.get("FastestLap") or {}
        fastest_lap = fl.get("lap", "")
        fastest_lap_rank = fl.get("rank", "")
        fastest_lap_time = (fl.get("Time") or {}).get("time", "")

        row = [
            driver_id,
            constructor_id,
            res.get("number"),
            res.get("grid"),
            res.get("position"),
            res.get("positionText"),
            res.get("position"),
            res.get("points"),
            res.get("laps"),
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
        data_table.append(row)

    logger.info(f"Prepared {len(data_table)} rows for year={year}, round={round_num}")

    # Convert staged rows into DataFrames with schema-aligned columns
    if driver_table:
        drivers_df = pd.DataFrame(
            driver_table,
            columns=[
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
    else:
        drivers_df = None

    if constructor_table:
        constructors_df = pd.DataFrame(
            constructor_table,
            columns=["constructorId", "constructorRef", "name", "nationality"],
        )
    else:
        constructors_df = None

    if status_table:
        statuses_df = pd.DataFrame(status_table, columns=["statusId", "status"])
    else:
        statuses_df = None

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

    # Write to database inside a single transaction (FK-safe order)
    with engine.begin() as conn:
        if drivers_df is not None and not drivers_df.empty:
            drivers_df.to_sql(
                "drivers", conn, if_exists="append", index=False, method="multi"
            )
            logger.info(f"Inserted {len(drivers_df)} new drivers")
        if constructors_df is not None and not constructors_df.empty:
            constructors_df.to_sql(
                "constructors", conn, if_exists="append", index=False, method="multi"
            )
            logger.info(f"Inserted {len(constructors_df)} new constructors")
        if statuses_df is not None and not statuses_df.empty:
            statuses_df.to_sql(
                "status", conn, if_exists="append", index=False, method="multi"
            )
            logger.info(f"Inserted {len(statuses_df)} new statuses")

        if not results_df.empty:
            results_df.to_sql(
                "race_data", conn, if_exists="append", index=False, method="multi"
            )
            logger.info(f"Inserted {len(results_df)} race result rows")

    return len(results_df)
