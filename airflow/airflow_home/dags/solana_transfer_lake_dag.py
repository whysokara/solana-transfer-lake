from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="solana_transfer_lake",
    description="End-to-end Solana transfer data pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["solana", "blockchain", "data-engineering"],
) as dag:

    fetch_raw_data = BashOperator(
    task_id="fetch_raw_data",
    bash_command="""
    cd {{ var.value.PROJECT_ROOT }} &&
    source .pavenv/bin/activate &&
    python scripts/fetch_helius_transfers.py --date {{ ds }}
    """
)



    csv_to_parquet = BashOperator(
    task_id="csv_to_parquet",
    bash_command="""
    cd {{ var.value.PROJECT_ROOT }} &&
    source .pavenv/bin/activate &&
    python jobs/csv_to_parquet.py --date {{ ds }}
    """
)


    silver_to_gold = BashOperator(
    task_id="silver_to_gold",
    bash_command="""
    cd {{ var.value.PROJECT_ROOT }} &&
    source .pavenv/bin/activate &&
    python jobs/silver_to_gold_daily.py --date {{ ds }}
    """
)


    fetch_raw_data >> csv_to_parquet >> silver_to_gold
