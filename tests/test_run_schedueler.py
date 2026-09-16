import unittest
from unittest.mock import patch

import pandas as pd

from f1_podium.flows import run_schedueler


class RunSchedulerTest(unittest.TestCase):
    def test_schedule_runs_delegates_to_fastf1_service(self):
        calls = []

        class FastF1Service:
            def get_next_races(self):
                calls.append("get_next_races")
                return pd.DataFrame({"RoundNumber": []})

        class Logger:
            def info(self, message):
                pass

        with (
            patch.object(run_schedueler, "FastF1Service", FastF1Service),
            patch.object(run_schedueler, "get_run_logger", lambda: Logger()),
        ):
            run_schedueler.schedule_runs.fn()

        self.assertEqual(calls, ["get_next_races"])
