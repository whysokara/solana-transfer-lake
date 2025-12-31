# Solana Transfer Lake

An end-to-end data engineering pipeline for analyzing high-frequency Solana payment and transfer activity.

## Overview

This project ingests real Solana on-chain transfer data, cleans and standardizes it using Apache Spark, and produces analytics-ready daily aggregates.

The pipeline follows a Bronze → Silver → Gold data lake architecture and is designed to be production-ready and cloud-portable.

## Data Flow

1. **Bronze (Raw)**
   - Source: Helius Solana API
   - Format: CSV
   - Content: Parsed wallet-to-wallet transfers

2. **Silver (Cleaned)**
   - Format: Parquet
   - Explicit schema enforcement
   - Filters only valid payment events
   - Adds event_date derived from timestamp

3. **Gold (Aggregated)**
   - Daily aggregates per event_date
   - Metrics:
     - Total transactions
     - Total transferred amount
     - Unique senders
     - Unique receivers

## Tech Stack

- Python 3.10
- Apache Spark (PySpark)
- Helius Solana API
- Local filesystem (S3-ready layout)
- Virtual environment based dependency isolation

## Project Structure
```
solana-transfer-lake/
├── data/
│ ├── raw/
│ ├── silver/
│ └── gold/
├── jobs/
│ ├── csv_to_parquet.py
│ └── silver_to_gold_daily.py
├── scripts/
│ └── fetch_helius_transfers.py
└── README.md
```
## How to Run

1. Fetch raw data:
   ```bash
   python scripts/fetch_helius_transfers.py 

2. Convert CSV to Parquet:
    ```bash
    python jobs/csv_to_parquet.py --date YYYY-MM-DD

3.  Build daily aggregates:
    ```bash
    python jobs/silver_to_gold_daily.py --date YYYY-MM-DD

## Notes
-   Ingestion date and event date are treated separately
-   All jobs are parameterized and idempotent
-   The pipeline can be lifted to S3 with minimal changes

## Future Work
-   Airflow orchestration
-   S3-backed data lake
-   Streaming ingestion
-   Dashboarding