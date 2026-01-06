import polars as pl
import requests

from weekly_cash_balances.db.db import DuckDB


def get_exchange_rates_per_date(
    dates_df: pl.DataFrame, pairs_df: pl.DataFrame
) -> pl.DataFrame:
    """
    Fetch exchange rates for given dates and currency pairs.
    """
    exchange_rates = []
    for date_row in dates_df.iter_rows(named=True):
        for pair_row in pairs_df.iter_rows(named=True):
            date = date_row["week_start"].strftime("%Y-%m-%d")
            print(
                f"Fetching exchange rate for date {date} and FX Pair {pair_row['base_currency']}/{pair_row['quote_currency']}"
            )

            response = requests.get(
                f"https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@{date}/v1/currencies/{pair_row['base_currency'].lower()}.json"
            )
            rates = response.json()[pair_row["base_currency"].lower()]
            exchange_rates.append(
                {
                    "currency_pair_id": pair_row["id"],
                    "value": rates[pair_row["quote_currency"].lower()],
                    "week_start": date_row["week_start"],
                }
            )

    return pl.DataFrame(exchange_rates)


def get_exchange_rates_data() -> None:
    with DuckDB() as db:
        dates_df = db.get_asset_values().select("week_start").unique()
        currency_pairs_df = db.get_currency_pairs()
        exchange_rates_df = get_exchange_rates_per_date(dates_df, currency_pairs_df)
        db.insert_data(exchange_rates_df, "exchange_rates")
