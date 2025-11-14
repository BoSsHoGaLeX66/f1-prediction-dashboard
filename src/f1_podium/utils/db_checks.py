"""Utility helpers for checking and resolving DB IDs.

These helpers encapsulate the logic for looking up existing circuit, driver,
constructor, and status IDs from reference DataFrames, and preparing new rows
when a referenced entity does not yet exist in the local database.
"""

from typing import Optional, Tuple, List
import pandas as pd


def next_id(df: pd.DataFrame, col: str) -> int:
    """Return the next integer id for a non-empty table else 1."""
    return int(df[col].max()) + 1 if not df.empty else 1


def resolve_circuit_id(
    circuits: pd.DataFrame, max_circuit: int, race: dict
) -> int:
    """Return the local ``circuitId`` for the given ``race``.

    Falls back to ``max_circuit`` if the circuit is not found.
    """
    circuit_ref = race["Circuit"]["circuitId"]
    try:
        circuit_id = circuits.loc[
            circuits["circuitRef"] == circuit_ref, "circuitId"
        ].values[0]
    except Exception:
        circuit_id = max_circuit
    return int(circuit_id)


def resolve_driver(
    drivers: pd.DataFrame, result_row: dict, next_driver_id: int
) -> Tuple[int, Optional[List], int]:
    """Find or stage-create a driver.

    Returns a tuple of ``(driver_id, new_row, next_driver_id)`` where
    ``new_row`` is a list representing a new driver row to append, or ``None``
    if the driver already exists.
    """
    driver_ref = result_row["Driver"]["driverId"]
    existing = drivers.loc[drivers["driverRef"] == driver_ref, "driverId"]
    if not existing.empty:
        return int(existing.values[0]), None, next_driver_id

    driver = result_row["Driver"]
    driver_id = next_driver_id
    new_row = [
        driver_id,
        driver_ref,
        result_row.get("number"),
        driver.get("code"),
        driver.get("givenName"),
        driver.get("familyName"),
        driver.get("dateOfBirth"),
        driver.get("nationality"),
    ]
    return driver_id, new_row, next_driver_id + 1


def resolve_constructor(
    constructors: pd.DataFrame, result_row: dict, next_constructor_id: int
) -> Tuple[int, Optional[List], int]:
    """Find or stage-create a constructor.

    Returns ``(constructor_id, new_row, next_constructor_id)``.
    """
    constructor_ref = result_row["Constructor"]["constructorId"]
    existing = constructors.loc[
        constructors["constructorRef"] == constructor_ref, "constructorId"
    ]
    if not existing.empty:
        return int(existing.values[0]), None, next_constructor_id

    constructor = result_row["Constructor"]
    constructor_id = next_constructor_id
    new_row = [
        constructor_id,
        constructor_ref,
        constructor.get("name"),
        constructor.get("nationality"),
    ]
    return constructor_id, new_row, next_constructor_id + 1


def resolve_status(
    statuses: pd.DataFrame, status_text: str, next_status_id: int
) -> Tuple[int, Optional[List], int]:
    """Find or stage-create a status row.

    Returns ``(status_id, new_row, next_status_id)``.
    """
    existing = statuses.loc[statuses["status"] == status_text, "statusId"]
    if not existing.empty:
        return int(existing.values[0]), None, next_status_id

    status_id = next_status_id
    new_row = [status_id, status_text]
    return status_id, new_row, next_status_id + 1

