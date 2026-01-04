"""Airflow DAG for NYC Taxi data ingestion."""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

import sys

sys.path.insert(0, "/opt/ingestion")

from download_tlc_data import TLCDataDownloader


# Default arguments for the DAG
default_args = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def download_taxi_data(taxi_type: str, year: int, month: int, **context) -> dict:
    """Download taxi data for a specific month."""
    downloader = TLCDataDownloader()
    success, message = downloader.download_month(
        taxi_type=taxi_type, year=year, month=month, skip_existing=True
    )

    result = {
        "taxi_type": taxi_type,
        "year": year,
        "month": month,
        "success": success,
        "message": message,
    }

    if not success:
        raise Exception(f"Download failed: {message}")

    return result


def download_monthly_data(**context):
    """Download data for the execution month."""
    # Get execution date (logical date in Airflow)
    execution_date = context["logical_date"]

    # Download previous month's data (as current month might not be complete)
    target_date = execution_date - timedelta(days=30)
    year = target_date.year
    month = target_date.month

    results = []

    # Download both yellow and green taxi data
    for taxi_type in ["yellow", "green"]:
        result = download_taxi_data(
            taxi_type=taxi_type, year=year, month=month, **context
        )
        results.append(result)

    return results


# Create the DAG
with DAG(
    dag_id="taxi_data_ingestion",
    default_args=default_args,
    description="Download NYC TLC taxi data to data lake",
    schedule_interval="@monthly",  # Run on the first day of each month
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["ingestion", "taxi", "tlc"],
) as dag:

    # Start task
    start = EmptyOperator(task_id="start")

    # Download task
    download = PythonOperator(
        task_id="download_monthly_data",
        python_callable=download_monthly_data,
    )

    # End task
    end = EmptyOperator(task_id="end")

    # Task dependencies
    start >> download >> end


# Create a manual/backfill DAG for downloading historical data
with DAG(
    dag_id="taxi_data_backfill",
    default_args=default_args,
    description="Backfill historical NYC TLC taxi data",
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["ingestion", "taxi", "tlc", "backfill"],
) as backfill_dag:

    start_backfill = EmptyOperator(task_id="start")

    # Create tasks for each month (Jan-Mar 2023 as example)
    download_tasks = []

    for year in [2023]:
        for month in [1, 2, 3]:
            for taxi_type in ["yellow", "green"]:
                task_id = f"download_{taxi_type}_{year}_{month:02d}"

                task = PythonOperator(
                    task_id=task_id,
                    python_callable=download_taxi_data,
                    op_kwargs={
                        "taxi_type": taxi_type,
                        "year": year,
                        "month": month,
                    },
                )
                download_tasks.append(task)

    end_backfill = EmptyOperator(task_id="end")

    # Run all downloads in parallel, then end
    start_backfill >> download_tasks >> end_backfill
