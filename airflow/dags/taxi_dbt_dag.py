"""Airflow DAG for dbt transformations."""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


default_args = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# dbt commands
DBT_PROJECT_DIR = "/opt/dbt"
DBT_PROFILES_DIR = "/opt/dbt"


with DAG(
    dag_id="taxi_dbt_pipeline",
    default_args=default_args,
    description="Run dbt models for taxi data warehouse",
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["dbt", "transformation", "warehouse"],
) as dag:

    start = EmptyOperator(task_id="start")

    # Install dbt dependencies
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir {DBT_PROFILES_DIR}",
    )

    # Load seed data
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt seed --profiles-dir {DBT_PROFILES_DIR}",
    )

    # Run staging models
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select staging --profiles-dir {DBT_PROFILES_DIR}",
    )

    # Run core models (dimensions and facts)
    dbt_run_core = BashOperator(
        task_id="dbt_run_core",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select marts.core --profiles-dir {DBT_PROFILES_DIR}",
    )

    # Run analytics models
    dbt_run_analytics = BashOperator(
        task_id="dbt_run_analytics",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select marts.analytics --profiles-dir {DBT_PROFILES_DIR}",
    )

    # Test all models
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR}",
    )

    # Generate documentation
    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt docs generate --profiles-dir {DBT_PROFILES_DIR}",
    )

    end = EmptyOperator(task_id="end")

    # Task dependencies
    (
        start
        >> dbt_deps
        >> dbt_seed
        >> dbt_run_staging
        >> dbt_run_core
        >> dbt_run_analytics
        >> dbt_test
        >> dbt_docs
        >> end
    )
