import panel as pn
import plotly.express as px

from f1_podium.db.connection import DatabaseConnection
from f1_podium.db.repositories import (
    DriverRepository,
    PredictionRepository,
    ProcessedDataRepository,
    RaceRepository,
)


pn.extension("plotly")


def get_data():
    """Get all database-backed data needed for the dashboard."""
    connection = DatabaseConnection.from_env()
    return (
        ProcessedDataRepository(connection).get_all(),
        DriverRepository(connection).get_all(),
        RaceRepository(connection).get_all(),
        PredictionRepository(connection).get_all(),
    )


df, drivers, df_results, df_predictions = get_data()
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

top_3s = pn.indicators.Number(name="Top 3 Percentage", value=0, format="{value:.2%}")

wins = pn.indicators.Number(name="Wins", value=0)

top_5s = pn.indicators.Number(name="Top 5 Finishes", value=0)


def update_driver_picker(year):
    driver_ids = df[df["year"] == year.new]
    driver_id_series = driver_ids["driverId"]
    drivers_this_year = driver_id_series.unique()

    year_drivers = driver_finder.loc[drivers_this_year.tolist()]
    driver_dict = year_drivers.set_index("driverRef")[["driverId"]].T.to_dict(
        "records"
    )[0]

    driver_picker.options = driver_dict


def update_plot(driver, year):
    temp_df = df[(df["driverId"] == driver) & (df["year"] == year)]
    fig = px.line(
        temp_df,
        x="round",
        y="top3_driver_season_percentage",
    )
    fig.update_traces(mode="lines+markers", marker=dict(size=10), line=dict(width=4))
    fig.layout.autosize = True

    top_3s.value = temp_df.iloc[-1]["top3_driver_season_percentage"]

    temp_results = df_results[
        (df_results["driverId"] == driver) & (df_results["year"] == year)
    ]

    wins_num = (temp_results["positionOrder"] == 1).sum()

    top_5_num = (
        (temp_results["positionOrder"] > 0) & (temp_results["positionOrder"] <= 5)
    ).sum()

    wins.value = wins_num
    top_5s.value = top_5_num

    return fig


year_picker.param.watch(update_driver_picker, "value")
year_picker.param.trigger("value")

plot = pn.bind(update_plot, driver_picker, year_picker)

indicator_style = {"width": "250px", "height": "150px", "justify-content": "center"}

indicator_layout = pn.FlexBox(
    *[
        pn.Card(
            top_3s,
            hide_header=True,
            styles=indicator_style,
        ),
        pn.Card(
            wins,
            hide_header=True,
            styles=indicator_style,
        ),
        pn.Card(
            top_5s,
            hide_header=True,
            styles=indicator_style,
        ),
    ],
    align_content="space-evenly",
    gap="50px",
    justify_content="center",
)

pn.template.FastListTemplate(
    title="F1 Data Dashboard",
    sidebar=["# Inputs", year_picker, driver_picker],
    main=["### Plot", indicator_layout, plot],
).servable()
