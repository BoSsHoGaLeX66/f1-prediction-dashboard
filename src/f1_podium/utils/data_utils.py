import pandas as pd


def top3_finishes(row: pd.Series, df: pd.DataFrame):
    top_3_count = df[
        (df.year == row.year)
        & (df["round"] < row["round"])
        & (df.driverId == row.driverId)
    ].top_3.sum()
    top_3_freq = (
        top_3_count
        / df[
            (df.year == row.year)
            & (df.driverId == row.driverId)
            & (df["round"] < row["round"])
        ].driverId.count()
    )

    return top_3_freq


def avg_finish_position_season(row: pd.Series, df: pd.DataFrame):
    return df.loc[
        (df.driverId == row.driverId)
        & (df.year == row.year)
        & (df["round"] < row["round"]),
        "positionOrder",
    ].mean()


def constructor_top_3(row: pd.Series, df: pd.DataFrame):
    top_3_count = df[
        (df["year"] == row.year)
        & (df.constructorId == row.constructorId)
        & (df["round"] < row["round"])
    ].top_3.sum()
    top_3_freq = (
        top_3_count
        / df[
            (df.year == row.year)
            & (df.constructorId == row.constructorId)
            & (df["round"] < row["round"])
        ].driverId.count()
    )
    return top_3_freq


def percent_top_3_at_circuit(row: pd.Series, df: pd.DataFrame):
    return (
        df[
            (df.circuitId == row.circuitId)
            & (df.driverId == row.driverId)
            & (df["year"] < row["year"])
        ].top_3.sum()
        / df[
            (df.circuitId == row.circuitId) & (df.driverId == row.driverId)
        ].circuitId.count()
    )
