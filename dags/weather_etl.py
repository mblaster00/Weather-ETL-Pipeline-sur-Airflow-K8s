from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from kubernetes.client import models as k8s

IMAGE = "europe-west1-docker.pkg.dev/weather-etl-airflow/weather-etl/weather-etl:latest"

default_args = {
    "owner": "meisterpod",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

env_vars = [
    k8s.V1EnvVar(
        name="CLOUDSQL_PASSWORD",
        value_from=k8s.V1EnvVarSource(
            secret_key_ref=k8s.V1SecretKeySelector(
                name="cloudsql-postgres-secret",
                key="password",
            )
        ),
    ),
    k8s.V1EnvVar(
        name="OPENWEATHER_API_KEY",
        value_from=k8s.V1EnvVarSource(
            secret_key_ref=k8s.V1SecretKeySelector(
                name="weather-api-secret",
                key="api_key",
            )
        ),
    ),
    k8s.V1EnvVar(
        name="CLOUDSQL_HOST",
        value_from=k8s.V1EnvVarSource(
            secret_key_ref=k8s.V1SecretKeySelector(
                name="cloudsql-secret",
                key="host",
            )
        ),
    ),
    k8s.V1EnvVar(
        name="CLOUDSQL_DATABASE",
        value_from=k8s.V1EnvVarSource(
            secret_key_ref=k8s.V1SecretKeySelector(
                name="cloudsql-secret",
                key="database",
            )
        ),
    ),
]

with DAG(
    dag_id="weather_etl",
    description="Hourly weather ETL pipeline — OpenWeatherMap to Cloud SQL",
    schedule_interval="@hourly",
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
        env_vars=env_vars,
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
        env_vars=env_vars,
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
        env_vars=env_vars,
        service_account_name="airflow-pods-ksa",
        is_delete_operator_pod=True,
        get_logs=True,
    )

    extract_task >> transform_task >> load_task