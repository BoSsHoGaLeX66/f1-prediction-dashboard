"""Repository classes for database tables."""

from f1_podium.db.repositories.circuits import CircuitRepository
from f1_podium.db.repositories.constructors import ConstructorRepository
from f1_podium.db.repositories.drivers import DriverRepository
from f1_podium.db.repositories.prediction import PredictionRepository
from f1_podium.db.repositories.processed_data import ProcessedDataRepository
from f1_podium.db.repositories.races import RaceRepository
from f1_podium.db.repositories.statuses import StatusRepository

__all__ = [
    "CircuitRepository",
    "ConstructorRepository",
    "DriverRepository",
    "PredictionRepository",
    "ProcessedDataRepository",
    "RaceRepository",
    "StatusRepository",
]
