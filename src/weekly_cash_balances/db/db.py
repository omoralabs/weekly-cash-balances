import os

import duckdb
import polars as pl
from dotenv import load_dotenv

load_dotenv()


class DuckDB:
    def __init__(self):
        self._setup_motherduck_token()
        self.db_name = "weekly_cash_balances"
        self.create_database()
        self.conn = duckdb.connect(f"md:{self.db_name}")

    def __enter__(self):
        self.conn.begin()
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        if exc_type is not None:
            self.conn.rollback()
        else:
            self.conn.commit()
        self.conn.close()
        return False

    def create_database(self):
        self.conn = duckdb.connect("md:")
        self.conn.execute(f"CREATE DATABASE IF NOT EXISTS {self.db_name}")

    def _setup_motherduck_token(self) -> None:
        """Set up MotherDuck token if available."""
        token = os.getenv("MOTHERDUCK_TOKEN")
        if token:
            os.environ["motherduck_token"] = token

    def setup_schema(self) -> None:
        print("Dropping existing tables...")
        self.conn.execute("DROP TABLE IF EXISTS asset_values CASCADE")
        self.conn.execute("DROP TABLE IF EXISTS assets CASCADE")
        self.conn.execute("DROP TABLE IF EXISTS exchange_rates CASCADE")
        self.conn.execute("DROP TABLE IF EXISTS currency_pairs CASCADE")
        self.conn.execute("DROP TABLE IF EXISTS currencies CASCADE")
        self.conn.execute("DROP TABLE IF EXISTS account_suppliers CASCADE")

        print("Dropping sequences...")
        self.conn.execute("DROP SEQUENCE IF EXISTS currencies_id_seq;")
        self.conn.execute("DROP SEQUENCE IF EXISTS asset_values_id_seq;")
        self.conn.execute("DROP SEQUENCE IF EXISTS currency_pairs_id_seq;")
        self.conn.execute("DROP SEQUENCE IF EXISTS account_suppliers_id_seq;")

        self.create_currencies_table()
        self.create_currency_pairs_table()
        self.create_account_suppliers_table()
        self.create_assets_table()
        self.create_assets_value_table()
        self.create_exchange_rates_table()
        self.create_indexes()

    def create_assets_table(self) -> None:
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS assets (
                id UUID PRIMARY KEY,
                currency_id INTEGER NOT NULL,
                supplier_id INTEGER NOT NULL,
                FOREIGN KEY (currency_id) REFERENCES currencies(id),
                FOREIGN KEY (supplier_id) REFERENCES account_suppliers(id)
            )
        """)

    def create_account_suppliers_table(self) -> None:
        self.conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS account_suppliers_id_seq START 1
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS account_suppliers (
                id INTEGER PRIMARY KEY DEFAULT nextval('account_suppliers_id_seq'),
                name VARCHAR NOT NULL,
            )
        """)

    def create_currencies_table(self) -> None:
        self.conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS currencies_id_seq START 1
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS currencies (
                id INTEGER PRIMARY KEY DEFAULT nextval('currencies_id_seq'),
                name VARCHAR NOT NULL
            )
        """)

    def create_assets_value_table(self) -> None:
        self.conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS asset_values_id_seq START 1
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS asset_values (
                id INTEGER PRIMARY KEY DEFAULT nextval('asset_values_id_seq'),
                asset_id UUID NOT NULL,
                value DECIMAL(10,2) NOT NULL,
                week_start TIMESTAMP NOT NULL,
                FOREIGN KEY (asset_id) REFERENCES assets(id)
            )
        """)

    def create_exchange_rates_table(self) -> None:
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS exchange_rates (
                currency_pair_id INTEGER NOT NULL,
                value DECIMAL(10,5) NOT NULL,
                week_start TIMESTAMP NOT NULL,
                PRIMARY KEY (currency_pair_id, week_start),
                FOREIGN KEY (currency_pair_id) REFERENCES currency_pairs(id)
            )
        """)

    def create_currency_pairs_table(self) -> None:
        self.conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS currency_pairs_id_seq START 1
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS currency_pairs(
                id INTEGER PRIMARY KEY DEFAULT nextval('currency_pairs_id_seq'),
                base_currency_id INTEGER NOT NULL,
                quote_currency_id INTEGER NOT NULL,
                FOREIGN KEY (base_currency_id) REFERENCES currencies(id),
                FOREIGN KEY (quote_currency_id) REFERENCES currencies(id)
            )
            """)

    def create_indexes(self) -> None:
        # Create indexes for better query performance
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_assets_currency_id
            ON assets(currency_id)
        """)

        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_assets_values_week_start
            ON asset_values(week_start)
        """)

        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_exchange_rates_week_start
            ON exchange_rates(week_start)
        """)

        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_asset_values_asset_week_start
            ON asset_values(asset_id, week_start)
        """)

    def insert_data(self, df: pl.DataFrame, table: str) -> None:
        cols = ", ".join(df.columns)
        self.conn.execute(
            f"""
            INSERT INTO {table} ({cols}) SELECT * FROM df
            """
        )

    def get_account_suppliers(self) -> pl.DataFrame:
        return self.conn.execute(
            """
            SELECT * FROM account_suppliers
            """
        ).pl()

    def get_asset_values(self) -> pl.DataFrame:
        return self.conn.execute(
            """
            SELECT * FROM asset_values
            """
        ).pl()

    def get_currency_pairs(self) -> pl.DataFrame:
        return self.conn.execute(
            """
            SELECT
                cp.id,
                c1.name as base_currency,
                c2.name as quote_currency
            FROM currency_pairs cp
            JOIN currencies c1 ON cp.base_currency_id = c1.id
            JOIN currencies c2 ON cp.quote_currency_id = c2.id
            """
        ).pl()
