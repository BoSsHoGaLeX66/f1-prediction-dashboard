"""Repository contracts exercised against an isolated SQL database."""

import unittest

import pandas as pd
from sqlalchemy import create_engine

from f1_podium.db.connection import DatabaseConnection
from f1_podium.db.repositories import (
    CircuitRepository,
    ConstructorRepository,
    DriverRepository,
    PredictionRepository,
    ProcessedDataRepository,
    RaceRepository,
    StatusRepository,
)


class RepositoryQueryTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        self.addCleanup(self.engine.dispose)
        self.connection = DatabaseConnection(self.engine)
        self.races = RaceRepository(self.connection)
        self.races.append(
            pd.DataFrame(
                [
                    [1, 10, 100, 2024, 1, 1, 1, 1],
                    [1, 20, 200, 2025, 1, 2, 2, 2],
                    [2, 10, 200, 2025, 1, 1, 3, 3],
                    [1, 10, 100, 2025, 2, 1, 4, 4],
                    [1, 10, 100, 2025, 3, 1, 5, 5],
                ],
                columns=[
                    "driverId",
                    "constructorId",
                    "circuitId",
                    "year",
                    "round",
                    "statusId",
                    "grid",
                    "positionOrder",
                ],
            )
        )

    def test_race_filters_intersect_and_preserve_all_matching_rows(self):
        result = self.races.get_filtered(
            driver_id=1, year=2025, circuit_id=100, team_id=10
        )
        self.assertEqual(result["round"].tolist(), [2, 3])
        self.assertEqual(result.columns.tolist(), self.races.get_all().columns.tolist())

    def test_reference_filters_match_one_race_without_duplicating_reference_rows(self):
        for repository_type, key, values in [
            (DriverRepository, "driverId", [1, 2, 3]),
            (ConstructorRepository, "constructorId", [10, 20, 30]),
            (CircuitRepository, "circuitId", [100, 200, 300]),
            (StatusRepository, "statusId", [1, 2, 3]),
        ]:
            with self.subTest(repository=repository_type.__name__):
                repository = repository_type(self.connection)
                repository.append(pd.DataFrame({key: values, "name": ["A", "B", "C"]}))
                result = repository.get_filtered(
                    driver_id=1, year=2025, circuit_id=100, team_id=10
                )
                self.assertEqual(
                    result.to_dict("records"), [{key: values[0], "name": "A"}]
                )
                self.assertTrue(
                    repository.get_filtered(driver_id=1, year=2024, team_id=20).empty
                )

    def test_processed_data_supports_the_same_race_dimensions(self):
        repository = ProcessedDataRepository(self.connection)
        repository.append(self.races.get_all().assign(feature=0.5))
        result = repository.get_filtered(
            driver_id=1, circuit_id=100, year=2025, team_id=10
        )
        self.assertEqual(result["round"].tolist(), [2, 3])
        self.assertEqual(result["feature"].tolist(), [0.5, 0.5])

    def test_prediction_circuit_matches_year_and_round_without_duplicate_predictions(
        self,
    ):
        repository = PredictionRepository(self.connection)
        repository.append(
            pd.DataFrame(
                [
                    [1, 10, 2024, 1, 0.1],
                    [1, 20, 2025, 1, 0.2],
                    [2, 10, 2025, 1, 0.3],
                    [1, 10, 2026, 1, 0.4],
                ],
                columns=["driverId", "constructorId", "year", "round", "podium"],
            )
        )
        result = repository.get_filtered(circuit_id=200, year=2025)
        self.assertEqual(result["podium"].tolist(), [0.2, 0.3])
        result = repository.get_filtered(
            driver_id=1, circuit_id=200, year=2025, team_id=20
        )
        self.assertEqual(result["podium"].tolist(), [0.2])
        self.assertEqual(repository.get_filtered(year=2026)["podium"].tolist(), [0.4])
        self.assertTrue(repository.get_filtered(year=2026, circuit_id=100).empty)

    def test_named_lookups_select_single_dimensions_and_driver_combinations(self):
        cases = [
            ("get_by_driver", (1,), [1, 1, 2, 3]),
            ("get_by_circuit", (200,), [1, 1]),
            ("get_by_year", (2024,), [1]),
            ("get_by_team", (20,), [1]),
            ("get_by_driver_and_year", (1, 2025), [1, 2, 3]),
            ("get_by_driver_and_circuit", (1, 100), [1, 2, 3]),
        ]
        for method, args, rounds in cases:
            with self.subTest(method=method):
                self.assertEqual(
                    getattr(self.races, method)(*args)["round"].tolist(), rounds
                )

    def populated_repositories(self):
        """Seed each table using its public write boundary."""
        for repository_type, key, values in [
            (DriverRepository, "driverId", [1, 2, 3]),
            (ConstructorRepository, "constructorId", [10, 20, 30]),
            (CircuitRepository, "circuitId", [100, 200, 300]),
            (StatusRepository, "statusId", [1, 2, 3]),
        ]:
            repository = repository_type(self.connection)
            repository.append(pd.DataFrame({key: values}))
            yield repository, key
        for repository_type in [PredictionRepository, ProcessedDataRepository]:
            repository = repository_type(self.connection)
            rows = self.races.get_all()
            if repository_type is PredictionRepository:
                rows = rows.drop(
                    columns=["circuitId", "statusId", "grid", "positionOrder"]
                )
            repository.append(rows)
            yield repository, "round"
        yield self.races, "round"

    def test_every_repository_supports_each_named_lookup(self):
        expected = {
            DriverRepository: [[1], [1, 2], [1], [1], [1], [1]],
            ConstructorRepository: [[10, 20], [10, 20], [10], [20], [10, 20], [10]],
            CircuitRepository: [[100, 200], [200], [100], [200], [100, 200], [100]],
            StatusRepository: [[1, 2], [1, 2], [1], [2], [1, 2], [1]],
            PredictionRepository: [
                [1, 1, 2, 3],
                [1, 1],
                [1],
                [1],
                [1, 2, 3],
                [1, 2, 3],
            ],
            ProcessedDataRepository: [
                [1, 1, 2, 3],
                [1, 1],
                [1],
                [1],
                [1, 2, 3],
                [1, 2, 3],
            ],
            RaceRepository: [[1, 1, 2, 3], [1, 1], [1], [1], [1, 2, 3], [1, 2, 3]],
        }
        methods = [
            ("get_by_driver", (1,)),
            ("get_by_circuit", (200,)),
            ("get_by_year", (2024,)),
            ("get_by_team", (20,)),
            ("get_by_driver_and_year", (1, 2025)),
            ("get_by_driver_and_circuit", (1, 100)),
        ]
        for repository, key in self.populated_repositories():
            for (method, args), values in zip(methods, expected[type(repository)]):
                with self.subTest(repository=type(repository).__name__, method=method):
                    self.assertCountEqual(
                        getattr(repository, method)(*args)[key].tolist(), values
                    )

    def test_omitted_filters_preserve_all_rows_and_unknown_values_preserve_schema(self):
        for repository, _ in self.populated_repositories():
            with self.subTest(repository=type(repository).__name__):
                original = repository.get_all()
                pd.testing.assert_frame_equal(repository.get_filtered(), original)
                pd.testing.assert_frame_equal(
                    repository.get_filtered(year=None), original
                )
                for key in ["driver_id", "circuit_id", "year", "team_id"]:
                    for value in [
                        0,
                        -1,
                        9999,
                        "1 OR 1=1",
                        "'; DROP TABLE race_results; --",
                    ]:
                        with self.subTest(filter=key, value=value):
                            result = repository.get_filtered(**{key: value})
                            self.assertTrue(result.empty)
                            self.assertEqual(
                                result.columns.tolist(), original.columns.tolist()
                            )
                pd.testing.assert_frame_equal(repository.get_all(), original)

    def test_local_reference_lookup_includes_rows_without_race_history(self):
        for repository_type, key, method in [
            (DriverRepository, "driverId", "get_by_driver"),
            (ConstructorRepository, "constructorId", "get_by_team"),
            (CircuitRepository, "circuitId", "get_by_circuit"),
        ]:
            with self.subTest(repository=repository_type.__name__):
                repository = repository_type(self.connection)
                repository.append(pd.DataFrame({key: [999]}))
                self.assertEqual(getattr(repository, method)(999)[key].tolist(), [999])
                self.assertTrue(repository.get_filtered(year=2025).empty)

    def test_empty_tables_return_empty_results_with_columns(self):
        with create_engine("sqlite:///:memory:").begin() as conn:
            connection = DatabaseConnection(conn.engine)
            schemas = {
                DriverRepository: ["driverId"],
                ConstructorRepository: ["constructorId"],
                CircuitRepository: ["circuitId"],
                StatusRepository: ["statusId"],
                RaceRepository: self.races.get_all().columns.tolist(),
                ProcessedDataRepository: self.races.get_all().columns.tolist(),
                PredictionRepository: ["driverId", "constructorId", "year", "round"],
            }
            for repository_type, columns in schemas.items():
                pd.DataFrame(columns=columns).to_sql(
                    repository_type.table_name, conn, index=False
                )
            for repository_type, columns in schemas.items():
                with self.subTest(repository=repository_type.__name__):
                    result = repository_type(connection).get_filtered(
                        driver_id=1, year=2025, circuit_id=100, team_id=10
                    )
                    self.assertTrue(result.empty)
                    self.assertEqual(result.columns.tolist(), columns)

    def test_existing_race_methods_retain_their_behavior(self):
        self.assertEqual(self.races.latest_round_for_year(2025), 3)
        self.assertIsNone(self.races.latest_round_for_year(2030))
        self.assertTrue(self.races.has_round(2025, 2))
        self.assertTrue(self.races.has_round(2025, 3))
        self.assertFalse(self.races.has_round(2025, 4))
        self.assertFalse(self.races.has_round(2030, 1))
        columns = [
            "driverId",
            "constructorId",
            "grid",
            "positionOrder",
            "statusId",
            "year",
            "round",
            "circuitId",
        ]
        pd.testing.assert_frame_equal(
            self.races.get_prediction_columns(), self.races.get_all()[columns]
        )

    def test_existing_append_supports_transactions_and_empty_inputs(self):
        for repository, _ in self.populated_repositories():
            with self.subTest(repository=type(repository).__name__):
                original = repository.get_all()
                repository.append(None)
                repository.append(pd.DataFrame())
                pd.testing.assert_frame_equal(repository.get_all(), original)
                with self.assertRaisesRegex(RuntimeError, "rollback"):
                    with self.connection.transaction() as conn:
                        repository.append(original.iloc[:1], conn=conn)
                        raise RuntimeError("rollback")
                pd.testing.assert_frame_equal(repository.get_all(), original)
                with self.connection.transaction() as conn:
                    repository.append(original.iloc[:1], conn=conn)
                self.assertEqual(len(repository.get_all()), len(original) + 1)
