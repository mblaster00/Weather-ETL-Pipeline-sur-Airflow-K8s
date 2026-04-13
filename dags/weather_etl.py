from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "omar",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="weather_etl",
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["etl", "weather"],
) as dag:

    extract_task = BashOperator(
        task_id="extract_weather",
        bash_command="echo 'extracting weather data'",
    )

    transform_task = BashOperator(
        task_id="transform_data",
        bash_command="echo 'transforming data'",
    )

    load_task = BashOperator(
        task_id="load_to_postgres",
        bash_command="echo 'loading to postgres'",
    )

    extract_task >> transform_task >> load_task