"""Data ingestion utilities for F1 race results.

This module contains Prefect tasks and helpers to fetch the latest race
results from the Ergast API and map them to local database identifiers.
"""

import pandas as pd
from prefect import task, flow
from prefect.logging import get_run_logger
from prefect.cache_policies import NO_CACHE
import requests
from sqlalchemy import text

# Robust import that works both as a package and as a script
try:
    from f1_podium.blocks.postgresql_conn import PostgresqlConnector
except (
    ModuleNotFoundError
):  # running as a script: python src/f1_podium/flows/process_data.py
    import sys
    from pathlib import Path

    sys.path.append(str(Path(__file__).resolve().parents[1]))  # add src/f1_podium
    from blocks.postgresql_conn import PostgresqlConnector

try:
    from f1_podium.utils.db_checks import (
        resolve_circuit_id,
        resolve_driver,
        resolve_constructor,
        resolve_status,
        next_id,
    )
except ModuleNotFoundError:  # running as a script
    import sys
    from pathlib import Path

    sys.path.append(str(Path(__file__).resolve().parents[1]))
    from utils.db_checks import (
        resolve_circuit_id,
        resolve_driver,
        resolve_constructor,
        resolve_status,
        next_id,
    )


@task(cache_policy=NO_CACHE)
def load_sql_data(engine):
    """Load reference tables required to map race data to local ids."""
    with engine.begin() as conn:
        circuits = pd.read_sql("SELECT * FROM circuits", conn)
        drivers = pd.read_sql("SELECT * FROM drivers", conn)
        constructors = pd.read_sql("SELECT * FROM constructors", conn)
        statuses = pd.read_sql("SELECT * FROM statuses", conn)
    return circuits, constructors, drivers, statuses


@task
def get_race_data():
    logger = get_run_logger()
    resp = requests.get(
        "https://api.jolpi.ca/ergast/f1/current/last/results/", timeout=15
    )
    resp.raise_for_status()
    payload = resp.json()

    race_table = payload.get("MRData", {}).get("RaceTable", {})
    races = race_table.get("Races", [])
    if not races:
        logger.warning("No races found in API response; nothing to do")
        return None, None

    race = races[0]
    return race, race_table


@flow(name="get_latest_race")
async def get_latest_race() -> int:
    """Fetch the latest race results and prepare rows for insertion.

    - Pulls the most recent race results from the Ergast API.
    - Loads local reference tables to map drivers, constructors, circuits, and status.
    - Skips work if the latest round for the current season already exists.

    Returns the count of prepared rows (0 if up to date).
    """
    logger = get_run_logger()

    race, race_table = get_race_data()

    # If the API doesn't return any data exit the flow
    if race is None:
        return 0

    # Prefer values from the race object; fall back to RaceTable
    year = int(race.get("season") or race_table.get("season"))
    round_num = int(race.get("round") or race_table.get("round"))

    # Resolve DB engine from Prefect Block
    postgres_block = await PostgresqlConnector.load("postgresdb")
    engine = postgres_block.get_engine()

    circuits, constructors, drivers, statuses = load_sql_data(engine)

    max_circuit = next_id(circuits, "circuitId")
    max_driver_id = next_id(drivers, "driverId")
    max_constructor_id = next_id(constructors, "constructorId")
    max_status_id = next_id(statuses, "statusId")

    # Skip if this round already exists for the season
    with engine.connect() as conn:
        existing_round = conn.execute(
            text("SELECT MAX(round) FROM race_results WHERE year = :year"),
            {"year": year},
        ).scalar()

    if existing_round is not None and int(existing_round) >= round_num:
        logger.info(
            f"Latest results already present for {year} round {round_num}; skipping"
        )
        return 0

    circuit_id = resolve_circuit_id(circuits, max_circuit, race)
    data_table = []  # rows for race_data
    driver_table = []  # new driver rows (only when not present)
    constructor_table = []  # new constructor rows (only when not present)
    status_table = []  # new status rows (only when not present)

    for res in race.get("Results", []):
        # Driver mapping
        driver_id, new_driver_row, max_driver_id = resolve_driver(
            drivers, res, max_driver_id
        )
        if new_driver_row is not None:
            driver_table.append(new_driver_row)

        # Constructor mapping
        constructor_id, new_constructor_row, max_constructor_id = resolve_constructor(
            constructors, res, max_constructor_id
        )
        if new_constructor_row is not None:
            constructor_table.append(new_constructor_row)

        # Status mapping
        status_text = res["status"]
        status_id, new_status_row, max_status_id = resolve_status(
            statuses, status_text, max_status_id
        )
        if new_status_row is not None:
            status_table.append(new_status_row)

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
                "statuses", conn, if_exists="append", index=False, method="multi"
            )
            logger.info(f"Inserted {len(statuses_df)} new statuses")

        if not results_df.empty:
            results_df.to_sql(
                "race_results", conn, if_exists="append", index=False, method="multi"
            )
            logger.info(f"Inserted {len(results_df)} race result rows")

    return results_df.shape[0]


if __name__ == "__main__":
    get_latest_race.serve()
