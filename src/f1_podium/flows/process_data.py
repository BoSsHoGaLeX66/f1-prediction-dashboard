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


def get_race_metadata(race: dict, race_table: dict) -> tuple[int, int]:
    """Extract the season year and round number from the API race payload."""
    year = int(race.get("season") or race_table.get("season"))
    round_num = int(race.get("round") or race_table.get("round"))
    return year, round_num


async def get_database_engine():
    """Load the configured PostgreSQL Prefect block and return its SQLAlchemy engine."""
    postgres_block = await PostgresqlConnector.load("postgresdb")
    return postgres_block.get_engine()


def get_next_reference_ids(
    circuits: pd.DataFrame,
    constructors: pd.DataFrame,
    drivers: pd.DataFrame,
    statuses: pd.DataFrame,
) -> dict[str, int]:
    """Calculate the next available ids for each reference table."""
    return {
        "circuit": next_id(circuits, "circuitId"),
        "driver": next_id(drivers, "driverId"),
        "constructor": next_id(constructors, "constructorId"),
        "status": next_id(statuses, "statusId"),
    }


def race_round_exists(engine, year: int, round_num: int) -> bool:
    """Return whether race results already include the target season round."""
    with engine.connect() as conn:
        existing_round = conn.execute(
            text("SELECT MAX(round) FROM race_results WHERE year = :year"),
            {"year": year},
        ).scalar()

    return existing_round is not None and int(existing_round) >= round_num


def get_result_time_fields(result: dict, status_text: str) -> tuple[str, str]:
    """Extract race time and milliseconds for completed or lapped results."""
    time_obj = result.get("Time")
    if status_text not in ["Finished", "Lapped"] or not time_obj:
        return "", ""

    return time_obj.get("time", ""), time_obj.get("millis", "")


def get_fastest_lap_fields(result: dict) -> tuple[str, str, str]:
    """Extract fastest lap number, rank, and lap time from a result payload."""
    fastest_lap_data = result.get("FastestLap") or {}
    fastest_lap_time = (fastest_lap_data.get("Time") or {}).get("time", "")
    return (
        fastest_lap_data.get("lap", ""),
        fastest_lap_data.get("rank", ""),
        fastest_lap_time,
    )


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
    race_time, millis = get_result_time_fields(result, status_text)
    fastest_lap, fastest_lap_rank, fastest_lap_time = get_fastest_lap_fields(result)

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


def stage_race_rows(
    race: dict,
    circuits: pd.DataFrame,
    constructors: pd.DataFrame,
    drivers: pd.DataFrame,
    statuses: pd.DataFrame,
    next_reference_ids: dict[str, int],
    year: int,
    round_num: int,
) -> tuple[list, list, list, list]:
    """Map API results to database rows and collect new reference rows."""
    max_driver_id = next_reference_ids["driver"]
    max_constructor_id = next_reference_ids["constructor"]
    max_status_id = next_reference_ids["status"]

    circuit_id = resolve_circuit_id(circuits, next_reference_ids["circuit"], race)
    data_table = []
    driver_table = []
    constructor_table = []
    status_table = []

    for result in race.get("Results", []):
        driver_id, new_driver_row, max_driver_id = resolve_driver(
            drivers, result, max_driver_id
        )
        if new_driver_row is not None:
            driver_table.append(new_driver_row)

        constructor_id, new_constructor_row, max_constructor_id = resolve_constructor(
            constructors, result, max_constructor_id
        )
        if new_constructor_row is not None:
            constructor_table.append(new_constructor_row)

        status_text = result["status"]
        status_id, new_status_row, max_status_id = resolve_status(
            statuses, status_text, max_status_id
        )
        if new_status_row is not None:
            status_table.append(new_status_row)

        data_table.append(
            build_race_result_row(
                result,
                driver_id,
                constructor_id,
                status_id,
                year,
                round_num,
                circuit_id,
            )
        )

    return data_table, driver_table, constructor_table, status_table


def dataframe_or_none(rows: list, columns: list[str]) -> pd.DataFrame | None:
    """Create a DataFrame from rows, or return None when no rows are staged."""
    if not rows:
        return None

    return pd.DataFrame(rows, columns=columns)


def build_insert_dataframes(
    data_table: list,
    driver_table: list,
    constructor_table: list,
    status_table: list,
) -> tuple[pd.DataFrame, pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None]:
    """Convert staged race and reference rows into schema-aligned DataFrames."""
    drivers_df = dataframe_or_none(
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
    constructors_df = dataframe_or_none(
        constructor_table,
        ["constructorId", "constructorRef", "name", "nationality"],
    )
    statuses_df = dataframe_or_none(status_table, ["statusId", "status"])

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


def insert_race_data(
    engine,
    results_df: pd.DataFrame,
    drivers_df: pd.DataFrame | None,
    constructors_df: pd.DataFrame | None,
    statuses_df: pd.DataFrame | None,
    logger,
) -> None:
    """Insert reference and race result DataFrames inside one database transaction."""
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

    year, round_num = get_race_metadata(race, race_table)

    engine = await get_database_engine()

    circuits, constructors, drivers, statuses = load_sql_data(engine)
    next_reference_ids = get_next_reference_ids(
        circuits, constructors, drivers, statuses
    )

    if race_round_exists(engine, year, round_num):
        logger.info(
            f"Latest results already present for {year} round {round_num}; skipping"
        )
        return 0

    data_table, driver_table, constructor_table, status_table = stage_race_rows(
        race,
        circuits,
        constructors,
        drivers,
        statuses,
        next_reference_ids,
        year,
        round_num,
    )

    logger.info(f"Prepared {len(data_table)} rows for year={year}, round={round_num}")

    results_df, drivers_df, constructors_df, statuses_df = build_insert_dataframes(
        data_table, driver_table, constructor_table, status_table
    )
    insert_race_data(
        engine, results_df, drivers_df, constructors_df, statuses_df, logger
    )

    return results_df.shape[0]


if __name__ == "__main__":
    get_latest_race.serve()
