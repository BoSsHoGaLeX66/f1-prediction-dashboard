"""Backward-compatible FastF1 Ergast service import."""

from fastf1.ergast import Ergast

from f1_podium.services.fastf1_service import FastF1Service


class FastF1ErgastService(FastF1Service):
    """Compatibility wrapper for callers that only need Ergast data."""

    def __init__(self, ergast_client: Ergast | None = None) -> None:
        super().__init__(ergast_client=ergast_client)
