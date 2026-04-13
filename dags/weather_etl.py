from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret

IMAGE = "europe-west1-docker.pkg.dev/weather-etl-airflow/weather-etl/weather-etl:latest"

default_args = {
    "owner": "omar",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

# References to Kubernetes Secrets — no actual values here
secrets = [
    Secret("env", "OPENWEATHER_API_KEY", "weather-api-secret", "api_key"),
    Secret("env", "CLOUDSQL_HOST", "cloudsql-secret", "host"),
    Secret("env", "CLOUDSQL_DATABASE", "cloudsql-secret", "database"),
    Secret("env", "CLOUDSQL_PASSWORD", "cloudsql-postgres-secret", "password"),
]

with DAG(
    dag_id="weather_etl",
    description="Hourly weather ETL pipeline — OpenWeatherMap to Cloud SQL",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["etl", "weather"],
) as dag:

    extract_task = KubernetesPodOperator(
        task_id="extract_weather",
        name="extract-weather-pod",
        namespace="airflow",
        image=IMAGE,
        cmds=["python", "-m", "utils.extract"],
        secrets=secrets,
        service_account_name="airflow-pods-ksa",
        is_delete_operator_pod=True,
        get_logs=True,
    )

    transform_task = KubernetesPodOperator(
        task_id="transform_data",
        name="transform-data-pod",
        namespace="airflow",
        image=IMAGE,
        cmds=["python", "-m", "utils.transform"],
        secrets=secrets,
        service_account_name="airflow-pods-ksa",
        is_delete_operator_pod=True,
        get_logs=True,
    )

    load_task = KubernetesPodOperator(
        task_id="load_to_postgres",
        name="load-postgres-pod",
        namespace="airflow",
        image=IMAGE,
        cmds=["python", "-m", "utils.load"],
        secrets=secrets,
        service_account_name="airflow-pods-ksa",
        is_delete_operator_pod=True,
        get_logs=True,
    )

    extract_task >> transform_task >> load_task