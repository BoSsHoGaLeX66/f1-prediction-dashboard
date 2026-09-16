from prefect import task, flow, get_run_logger
from prefect.deployments.flow_runs import run_deployment
from datetime import timedelta
import pandas as pd

try:
    from f1_podium.services import FastF1Service
except ModuleNotFoundError:  # running as a script
    import sys
    from pathlib import Path

    sys.path.append(str(Path(__file__).resolve().parents[2]))  # add src
    from f1_podium.services import FastF1Service


@task
def schedule_next_runs(next_race: pd.DataFrame):
    t = next_race["Session5DateUtc"].iat[0]
    run_deployment(
        name="run_pred/run_pred",
        scheduled_time=t - timedelta(hours=20),
        as_subflow=False,
        timeout=0,
        parameters={"round": next_race["RoundNumber"]},
    )


@flow(name="schedule_runs")
def schedule_runs():
    logger = get_run_logger()
    next_race = FastF1Service().get_next_races()
    logger.info("Got the next races")

    if next_race.shape[0] >= 1:
        logger.info("Found race in next week")
        schedule_next_runs(next_race)
    else:
        logger.info("No race in the next week")


if __name__ == "__main__":
    schedule_runs.serve()
