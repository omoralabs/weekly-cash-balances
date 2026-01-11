# weekly-cash-balances

Weekly cash balance tracking with multi-currency reporting built on DuckDB and dbt.

## Overview

Tracks weekly asset values across multiple currencies with automatic exchange rate conversion. Uses dbt for transforming raw data into reporting-ready views with currency conversion.

## Features

- **Multi-currency support** - Track assets in different currencies with automatic conversion
- **Weekly time-series** - Historical asset valuations and exchange rates by week
- **Bidirectional rates** - Automatic inverse exchange rate calculation
- **dbt transformations** - Layered models for clean analytics
- **MotherDuck integration** - Cloud-based DuckDB storage
- **Transaction safety** - Automatic rollback on failures

## Database Schema

### Raw Tables

- `currencies` - Currency definitions (USD, EUR, GBP, CHF)
- `account_suppliers` - Account provider names
- `assets` - Asset records with currency and supplier references
- `asset_values` - Weekly asset values with timestamps
- `currency_pairs` - Currency pairs linked to table currencies
- `exchange_rates` - Weekly exchange rates per currency pair

### dbt Models

**Intermediate:**
- `asset_data` - Assets joined with currency and supplier information
- `base_rates_data` - Exchange rates with currency names joined
- `reporting_rates` - Bidirectional rates (original + inverse pairs)
- `currency_amounts` - Weekly totals summed per currency

**Final:**
- `asset_values_data` - Weekly asset values with full details
- `converted_cash_balances` - All currency amounts converted to all reporting currencies
- `grouped_cash_balances` - Aggregated converted balances per currency

## Setup

### Prerequisites

- Python 3.13+
- uv package manager

### Installation

```bash
uv sync
```

### Database

Set `MOTHERDUCK_TOKEN` in `.env` for cloud database access.

### Generate Data

```bash
uv run src/weekly_cash_balances/main.py
```

This will:
1. Connect to MotherDuck (or create local database)
2. Set up all tables with foreign key relationships
3. Generate synthetic weekly financial data
4. Load data into DuckDB

### Run dbt Models

```bash
cd weekly_cash_balances_dbt
uv run dbt run
```

## Project Structure

```
weekly-cash-balances/
├── src/weekly_cash_balances/  # Python data generation
│   ├── data_gen/              # Synthetic data generators
│   ├── db/                    # Database setup and operations
│   └── main.py                # Data generation orchestration
└── weekly_cash_balances_dbt/  # dbt project
    ├── models/
    │   ├── intermediate/      # Base rates, reporting rates, currency amounts
    │   ├── asset_values_data.sql
    │   ├── converted_cash_balances.sql
    │   └── grouped_cash_balances.sql
    └── dbt_project.yml
```

## License

This project is licensed under the [MIT License](https://opensource.org/license/MIT)
