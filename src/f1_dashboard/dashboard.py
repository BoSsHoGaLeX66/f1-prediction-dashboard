import os
from dotenv import load_dotenv
import pandas as pd
import panel as pn
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from typing import Optional
import plotly.express as px


pn.extension("plotly")


def _get_env_var(*keys: str, default: Optional[str] = None) -> Optional[str]:
    """Return the first populated environment variable for the provided keys."""
    for key in keys:
        value = os.getenv(key)
        if value:
            return value
    return default


def get_engine() -> Engine:
    """Build a SQLAlchemy engine using .env values, allowing .env to override OS vars."""
    load_dotenv(override=True)

    host = _get_env_var("POSTGRES_HOST", "HOST")
    user = _get_env_var("POSTGRES_USER", "USER")
    password = _get_env_var("POSTGRES_PASSWORD", "PASSWORD")
    db_name = _get_env_var("POSTGRES_DB", "DB_NAME", default="f1_prediction")
    port = _get_env_var("POSTGRES_PORT", "PORT", default="5432")

    missing = [
        key
        for key, val in {"host": host, "user": user, "password": password}.items()
        if not val
    ]
    if missing:
        raise RuntimeError(f"Missing required database env vars: {', '.join(missing)}")

    return create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db_name}")


def get_data():
    engine = get_engine()
    return pd.read_sql("SELECT * FROM processed_race_data", engine), pd.read_sql(
        "SELECT * FROM drivers", engine
    )


df, drivers = get_data()
driver_finder = drivers.set_index("driverId", drop=False)

driver_names = drivers.set_index("driverRef")

year_picker = pn.widgets.IntInput(
    description="Input the year",
    name="Year",
    value=2025,
    start=df["year"].min(),
    end=df["year"].max(),
)

driver_picker = pn.widgets.Select(
    description="Select A Driver",
    name="Driver",
    options=driver_names[["driverId"]].T.to_dict("records")[0],
)


def update_driver_picker(year):
    driver_ids = df[df["year"] == year.new]
    driver_id_series = driver_ids["driverId"]
    drivers_this_year = driver_id_series.unique()

    year_drivers = driver_finder.loc[drivers_this_year.tolist()]
    driver_dict = year_drivers.set_index("driverRef")[["driverId"]].T.to_dict(
        "records"
    )[0]

    driver_picker.options = driver_dict
    print("Updated")


def update_plot(driver, year):
    fig = px.line(
        df[(df["driverId"] == driver) & (df["year"] == year)],
        x="round",
        y="top3_driver_season_percentage",
    )
    fig.update_traces(mode="lines+markers", marker=dict(size=10), line=dict(width=4))
    fig.layout.autosize = True

    return fig


year_picker.param.watch(update_driver_picker, "value")
year_picker.param.trigger("value")

plot = pn.bind(update_plot, driver_picker, year_picker)

pn.template.FastListTemplate(
    title="F1 Data Dashboard",
    sidebar=["# Inputs", year_picker, driver_picker],
    main=["### Plot", plot],
).servable()
