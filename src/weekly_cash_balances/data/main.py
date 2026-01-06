from weekly_cash_balances.data.assets import Assets
from weekly_cash_balances.data.ex_rates import get_exchange_rates_data
from weekly_cash_balances.data.utils import get_df_from_json, get_mondays_2025
from weekly_cash_balances.db.db import DuckDB


def insert_currencies() -> None:
    path = "src/weekly_cash_balances/data/sample/currencies.json"
    df = get_df_from_json(path)
    with DuckDB() as db:
        db.insert_data(df, "currencies")


def insert_currency_pairs() -> None:
    path = "src/weekly_cash_balances/data/sample/currency_pairs.json"
    df = get_df_from_json(path)
    with DuckDB() as db:
        db.insert_data(df, "currency_pairs")


def create_sample_data() -> None:
    try:
        print("Inserting currencies...")
        insert_currencies()
        print("Inserting currency pairs...")
        insert_currency_pairs()
        dates_df = get_mondays_2025()
        print("Inserting assets and asset values...")
        Assets(dates_df)
        print("Inserting exchange rates...")
        get_exchange_rates_data()
    except Exception as e:
        print(f"Failure creating sample data: {e}")
        raise
