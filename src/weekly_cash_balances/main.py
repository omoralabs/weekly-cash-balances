from weekly_cash_balances.data.main import create_sample_data
from weekly_cash_balances.db.db import DuckDB


def setup_data() -> None:
    """
    Set up the database schema for assets and its data.
    """
    try:
        with DuckDB() as db:
            print("Starting process...")
            db.setup_schema()
        print("Creating and filling in tables with sample data...")
        create_sample_data()
    except Exception as e:
        print(f"Failure setting up db schema and filling it in with sample data: {e}")
        raise


if __name__ == "__main__":
    setup_data()
