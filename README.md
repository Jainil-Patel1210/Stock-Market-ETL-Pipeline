# Stock Market ETL Pipeline

An ETL pipeline built with PySpark that ingests, transforms, and loads multi-year stock price data into a normalized PostgreSQL warehouse — with windowed metrics, data validation, and idempotent loading.

## Overview

This project simulates a small-scale data engineering pipeline for financial time-series data. It covers the three core ETL stages:

- **Extract** — reads raw multi-year OHLCV (Open, High, Low, Close, Volume) stock data from CSV source files.
- **Transform** — uses PySpark to compute windowed metrics (moving averages, daily returns) and validate records for nulls, duplicates, and schema mismatches.
- **Load** — writes cleaned, transformed data into a normalized PostgreSQL schema using incremental/idempotent loading, so re-running the pipeline doesn't create duplicate records.

## Architecture

```
CSV (raw stock data)
      │
      ▼
  PySpark (Extract + Transform)
      │  - schema validation
      │  - null / duplicate handling
      │  - windowed calculations (moving avg, daily returns)
      ▼
  PostgreSQL (Load)
      │  - dimension tables (stocks, dates)
      │  - fact table (daily_prices)
      │  - upsert logic to prevent duplicate inserts
      ▼
  Query-ready warehouse
```

## Database Schema

- **`dim_stocks`** — stock/ticker metadata (symbol, company name, sector)
- **`dim_dates`** — date dimension for time-based analysis
- **`fact_daily_prices`** — OHLCV values plus derived metrics (moving average, daily return), foreign-keyed to the dimension tables

## Tech Stack

- **Python** — orchestration and scripting
- **PySpark** — distributed data transformation
- **PostgreSQL** — data warehouse
- **SQL** — schema design and querying

## Key Design Decisions

- **Windowed transformations in Spark**: Moving averages and daily returns are computed using Spark's `Window` functions (partitioned by ticker, ordered by date) rather than in Pandas, to demonstrate distributed-computation patterns that scale to larger multi-ticker datasets.
- **Star-schema-style modeling**: Splitting data into dimension and fact tables (rather than one flat table) keeps the warehouse queryable and extensible — new metrics can be added to the fact table without touching dimension data.
- **Idempotent loading**: The load step upserts on a natural key (ticker + date) instead of blind inserts, so the pipeline can be safely re-run on the same data without creating duplicates — a common requirement in real-world daily batch pipelines.

## Setup

```bash
# Clone the repo
git clone https://github.com/JainilPatel/stock-market-etl-pipeline.git
cd stock-market-etl-pipeline

# Install dependencies
pip install -r requirements.txt

# Set up PostgreSQL connection
# Update config with your DB credentials

# Run the pipeline
python run_pipeline.py
```

## Future Improvements

- Orchestrate runs with Airflow instead of manual/script-based execution
- Add automated data quality tests (e.g., Great Expectations)
- Extend to streaming ingestion for near-real-time price updates

## Author

Jainil Patel — [GitHub](https://github.com/JainilPatel) · [LinkedIn](https://linkedin.com/in/JainilPatel)
