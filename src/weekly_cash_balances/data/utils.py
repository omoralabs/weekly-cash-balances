import json
from datetime import datetime

import polars as pl


# Generate all Mondays in 2025
def get_mondays_2025() -> pl.DataFrame:
    return (
        pl.date_range(
            datetime(2025, 1, 5),
            datetime(2025, 12, 31),
            interval="1w",
            eager=True,  # eager executes immediately
        )
        .alias("week_start")
        .to_frame()  # publishes it as a DataFrame not a Series
    )


def get_json(path: str) -> dict:
    with open(f"{path}") as f:
        return json.load(f)


def get_df_from_json(path: str) -> pl.DataFrame:
    json_file = get_json(path)
    return pl.DataFrame(json_file)
