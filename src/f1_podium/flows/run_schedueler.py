from prefect import task, flow, get_run_logger
from prefect.deployments.flow_runs import run_deployment
import fastf1
from datetime import datetime, timedelta
import pandas as pd


@task
def get_schedule():
    schedule = fastf1.get_event_schedule(datetime.now().year, include_testing=False)
    curr_date = datetime.utcnow()
    search_date = curr_date + timedelta(days=7)

    next_race = schedule[
        (schedule["Session5DateUtc"] > curr_date)
        & (schedule["Session5DateUtc"] < search_date)
    ]

    return next_race


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
    next_race = get_schedule()
    logger.info("Got the next races")

    if next_race.shape[0] >= 1:
        logger.info("Found race in next week")
        schedule_next_runs(next_race)
    else:
        logger.info("No race in the next week")


if __name__ == "__main__":
    schedule_runs.serve()

