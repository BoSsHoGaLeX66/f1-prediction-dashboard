import unittest

import pandas as pd

from f1_podium.features.preprocessor import Preprocessor


class PreprocessorTest(unittest.TestCase):
    def test_clean_quali_data_preserves_existing_output_shape(self):
        qualifying_data = pd.DataFrame(
            [
                {
                    "driverId": "verstappen",
                    "constructorId": "red_bull",
                    "grid": 1,
                    "Q1": "1:20.000",
                    "Q2": "1:19.000",
                    "Q3": "1:18.000",
                    "driverNumber": 1,
                    "driverCode": "VER",
                    "driverUrl": "driver-url",
                    "givenName": "Max",
                    "familyName": "Verstappen",
                    "dateOfBirth": "1997-09-30",
                    "driverNationality": "Dutch",
                    "constructorUrl": "constructor-url",
                    "constructorName": "Red Bull",
                    "constructorNationality": "Austrian",
                    "year": 2026,
                    "round": 12,
                }
            ]
        )
        race_description = pd.DataFrame({"circuitId": ["silverstone"]})
        drivers = pd.DataFrame(
            [{"driverId": 33, "driverRef": "verstappen"}]
        )
        circuits = pd.DataFrame(
            [{"circuitId": 9, "circuitRef": "silverstone"}]
        )
        constructors = pd.DataFrame(
            [{"constructorId": 1, "constructorRef": "red_bull"}]
        )

        result = Preprocessor(
            drivers=drivers,
            circuits=circuits,
            constructors=constructors,
        ).clean_quali_data(qualifying_data, race_description)

        self.assertEqual(
            result.drop(columns=["positionOrder", "statusId"]).to_dict(
                orient="records"
            ),
            [
                {
                    "driverId": 33,
                    "constructorId": 1,
                    "grid": 1,
                    "year": 2026,
                    "round": 12,
                    "circuitId": 9,
                }
            ],
        )
        self.assertTrue(result["positionOrder"].isna().all())
        self.assertTrue(result["statusId"].isna().all())

    def test_get_circuit_id_uses_next_id_for_unknown_circuit(self):
        preprocessor = Preprocessor(
            drivers=pd.DataFrame(),
            circuits=pd.DataFrame(
                [
                    {"circuitId": 9, "circuitRef": "silverstone"},
                    {"circuitId": 10, "circuitRef": "monza"},
                ]
            ),
            constructors=pd.DataFrame(),
        )

        self.assertEqual(preprocessor.get_circuit_id("spa"), 11)
