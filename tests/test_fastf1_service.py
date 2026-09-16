from datetime import datetime, timedelta
import unittest

import pandas as pd

from f1_podium.services import FastF1Service


class QualifyingResponse:
    content = [pd.DataFrame({"position": [1], "driverId": ["verstappen"]})]
    description = pd.DataFrame({"season": [2026], "round": [12]})


class ErgastClient:
    def get_qualifying_results(self, *args, **kwargs):
        assert args == ("current", "last")
        assert kwargs == {"result_type": "pandas"}
        return QualifyingResponse()

    def get_race_results(self, *args, **kwargs):
        assert args == ("current", "last")
        assert kwargs == {"result_type": "raw", "auto_cast": False}
        return [{"season": "2026", "round": "12", "Results": []}]


class FastF1ServiceTest(unittest.TestCase):
    def test_get_next_races_matches_scheduler_filtering(self):
        current_date = datetime(2026, 7, 16, 12, 0, 0)
        schedule = pd.DataFrame(
            {
                "RoundNumber": [1, 2, 3],
                "Session5DateUtc": [
                    current_date - timedelta(hours=1),
                    current_date + timedelta(days=3),
                    current_date + timedelta(days=8),
                ],
            }
        )
        calls = []

        def event_schedule_provider(year, include_testing=False):
            calls.append((year, include_testing))
            return schedule

        service = FastF1Service(
            ergast_client=ErgastClient(),
            event_schedule_provider=event_schedule_provider,
        )

        next_races = service.get_next_races(current_date=current_date)

        self.assertEqual(calls, [(datetime.now().year, False)])
        self.assertEqual(next_races["RoundNumber"].tolist(), [2])

    def test_get_quali_data_preserves_prediction_flow_shape(self):
        service = FastF1Service(ergast_client=ErgastClient())

        qualifying_results, race_description = service.get_quali_data()

        self.assertEqual(
            qualifying_results.to_dict(orient="records"),
            [
                {
                    "grid": 1,
                    "driverId": "verstappen",
                    "year": 2026,
                    "round": 12,
                }
            ],
        )
        self.assertEqual(
            race_description.to_dict(orient="records"),
            [{"season": 2026, "round": 12}],
        )

    def test_get_race_data_preserves_process_flow_payload_shape(self):
        service = FastF1Service(ergast_client=ErgastClient())

        race, race_table = service.get_race_data()

        self.assertEqual(race, {"season": "2026", "round": "12", "Results": []})
        self.assertEqual(
            race_table,
            {"season": "2026", "round": "12", "Races": [race]},
        )
