# Solana Transfer Lake

End-to-end data engineering pipeline for ingesting, transforming, and aggregating **Solana on-chain transfer data** using Python, Spark, and Airflow.

The project follows a **Bronze → Silver → Gold** lakehouse architecture and is designed to be deterministic, backfillable, and production-aligned.

---

## Architecture Overview

### Flow

1. Fetch raw Solana transfer data (CSV)
2. Convert raw CSV → typed Parquet (Silver)
3. Aggregate daily metrics (Gold)
4. Orchestrated via Apache Airflow

### Storage (local-first, cloud-ready)

```
data/
├── raw/
│   └── transfers/date=YYYY-MM-DD/
├── silver/
│   └── transfers/date=YYYY-MM-DD/
└── gold/
    └── transfers_daily/date=YYYY-MM-DD/
```

---

## Project Structure

```
solana-transfer-lake/
├── airflow/
│   ├── airflow_home/            # Airflow metadata (NOT committed)
│   └── dags/
│       └── solana_transfer_lake_dag.py
├── data/
│   ├── raw/
│   ├── silver/
│   └── gold/
├── jobs/
│   ├── csv_to_parquet.py
│   └── silver_to_gold_daily.py
├── scripts/
│   └── fetch_helius_transfers.py
├── requirements.txt
├── requirements-airflow.txt
├── .env                         # NOT committed
└── README.md
```

---

## Virtual Environments (Important)

This project intentionally uses **two separate virtual environments**.

### 1️⃣ Pipeline Environment (Spark + ingestion)

Used for:
- API ingestion
- Spark jobs
- Local testing

Create and activate:

```bash
python3.10 -m venv .pavenv
source .pavenv/bin/activate
pip install -r requirements.txt
```

---

### 2️⃣ Airflow Environment (orchestration only)

Used only for:
- Airflow scheduler
- Airflow webserver
- DAG execution

Create and activate:

```bash
python3.11 -m venv airflow_venv
source airflow_venv/bin/activate
pip install -r requirements-airflow.txt
```

**Do not mix these environments.**  
Airflow orchestrates jobs via shell commands; it does not import Spark code.

---

## Dependencies

### `requirements.txt` (pipeline)

```
pyspark
requests
pandas
python-dotenv
```

### `requirements-airflow.txt` (orchestration)

```
apache-airflow==2.9.3
```

---

## Environment Variables

### `.env` file (local only)

Create a `.env` file in the project root:

```
HELIUS_API_KEY=your_api_key_here
```

This file is **not committed**.  
The ingestion script reads the API key from the environment.

---

## Airflow Configuration

### AIRFLOW_HOME

Set Airflow home inside the project:

```bash
export AIRFLOW_HOME=$(pwd)/airflow/airflow_home
```

---

### Required Airflow Variable

The DAG expects a variable called `PROJECT_ROOT`.

Set it once:

```bash
airflow variables set PROJECT_ROOT "/absolute/path/to/solana-transfer-lake"
```

Example:

```bash
airflow variables set PROJECT_ROOT "/Users/kara/Desktop/solana-transfer-lake"
```

This allows the DAG to be portable across machines.

---

## Running the Pipeline

### 1️⃣ Fetch raw data (manual)

```bash
source .pavenv/bin/activate
python scripts/fetch_helius_transfers.py --date 2026-01-01
```

---

### 2️⃣ CSV → Parquet (Silver)

```bash
python jobs/csv_to_parquet.py --date 2026-01-01
```

---

### 3️⃣ Silver → Gold aggregates

```bash
python jobs/silver_to_gold_daily.py --date 2026-01-01
```

---

### 4️⃣ Run via Airflow

Start services:

```bash
airflow scheduler
airflow webserver --port 8080
```

Open in browser:

```
http://localhost:8080
```

Trigger the DAG **`solana_transfer_lake`**.

---

## Key Design Decisions

- Logical date (`{{ ds }}`) drives all partitions
- Ingestion, transformation, and aggregation are date-aligned
- Airflow contains no business logic
- Spark jobs are reusable outside Airflow
- Storage layer is swappable (local → S3)

---

## Next Steps

- Move raw and parquet storage to S3
- Use Airflow Connections for secrets
- Add retries, SLAs, and alerts
- Add data quality checks

---

## Notes

- `airflow_home/`, `.env`, and virtual environment folders are excluded from Git
- This project is designed to mirror real production workflows

**Twitter / X:** [@whysokara](https://x.com/whysokara)
