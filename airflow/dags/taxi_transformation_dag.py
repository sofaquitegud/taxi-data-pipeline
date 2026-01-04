"""Airflow DAG for taxi data transformation."""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

import sys

sys.path.insert(0, "/opt/spark/utils")
sys.path.insert(0, "/opt/spark/jobs")


default_args = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def run_cleaning_job(taxi_type: str, year: int, month: int, **context):
    """Run the data cleaning Spark job."""
    from spark_session import get_spark_session, stop_spark_session
    from clean_taxi_data import TaxiDataCleaner

    spark = get_spark_session(f"TaxiCleaning_{taxi_type}_{year}_{month}")

    try:
        cleaner = TaxiDataCleaner(spark)
        result = cleaner.process(taxi_type, year, month)
        return result
    finally:
        stop_spark_session(spark)


def run_feature_engineering(taxi_type: str, year: int, month: int, **context):
    """Run feature engineering Spark job."""
    from spark_session import get_spark_session, stop_spark_session
    from feature_engineering import FeatureEngineer

    spark = get_spark_session(f"TaxiFeatures_{taxi_type}_{year}_{month}")

    try:
        engineer = FeatureEngineer(spark)
        result = engineer.process(taxi_type, year, month)
        return result
    finally:
        stop_spark_session(spark)


# Main transformation DAG
with DAG(
    dag_id="taxi_data_transformation",
    default_args=default_args,
    description="Transform and clean taxi data",
    schedule_interval="@monthly",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["transformation", "taxi", "spark"],
) as dag:

    start = EmptyOperator(task_id="start")

    # Clean yellow taxi data
    clean_yellow = PythonOperator(
        task_id="clean_yellow_taxi",
        python_callable=run_cleaning_job,
        op_kwargs={
            "taxi_type": "yellow",
            "year": 2023,
            "month": 1,
        },
    )

    # Feature engineering for yellow taxi
    features_yellow = PythonOperator(
        task_id="features_yellow_taxi",
        python_callable=run_feature_engineering,
        op_kwargs={
            "taxi_type": "yellow",
            "year": 2023,
            "month": 1,
        },
    )

    end = EmptyOperator(task_id="end")

    start >> clean_yellow >> features_yellow >> end


# Backfill transformation DAG
with DAG(
    dag_id="taxi_transformation_backfill",
    default_args=default_args,
    description="Backfill transformation for historical data",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["transformation", "taxi", "backfill"],
) as backfill_dag:

    start_backfill = EmptyOperator(task_id="start")

    # Yellow taxi - January 2023
    clean_yellow_jan = PythonOperator(
        task_id="clean_yellow_2023_01",
        python_callable=run_cleaning_job,
        op_kwargs={"taxi_type": "yellow", "year": 2023, "month": 1},
    )

    features_yellow_jan = PythonOperator(
        task_id="features_yellow_2023_01",
        python_callable=run_feature_engineering,
        op_kwargs={"taxi_type": "yellow", "year": 2023, "month": 1},
    )

    end_backfill = EmptyOperator(task_id="end")

    start_backfill >> clean_yellow_jan >> features_yellow_jan >> end_backfill
