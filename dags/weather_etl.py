from datetime import datetime, timedelta
import requests
import json
import os
import psycopg2

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    "owner": "omar",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

def extract(**context):
    api_key = Variable.get("openweather_api_key")
    response = requests.get(
        "https://api.openweathermap.org/data/2.5/weather",
        params={"q": "Paris", "appid": api_key, "units": "metric"}
    )
    response.raise_for_status()
    context["ti"].xcom_push(key="raw_data", value=response.json())
    print("Extract successful")

def transform(**context):
    raw = context["ti"].xcom_pull(key="raw_data", task_ids="extract_weather")
    transformed = {
        "city": raw["name"],
        "temperature": raw["main"]["temp"],
        "humidity": raw["main"]["humidity"],
        "description": raw["weather"][0]["description"],
        "recorded_at": raw["dt"],
    }
    context["ti"].xcom_push(key="transformed_data", value=transformed)
    print(f"Transform successful — {transformed['city']} {transformed['temperature']}°C")

def load(**context):
    data = context["ti"].xcom_pull(key="transformed_data", task_ids="transform_data")

    hook = PostgresHook(postgres_conn_id="postgres_weather")
    hook.run("""
        INSERT INTO weather_data (city, temperature, humidity, description, recorded_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (city, recorded_at) DO NOTHING
    """, parameters=(
        data["city"],
        data["temperature"],
        data["humidity"],
        data["description"],
        datetime.fromtimestamp(data["recorded_at"]),
    ))
    print(f"Load successful — {data['city']} inserted into PostgreSQL")

with DAG(
    dag_id="weather_etl",
    description="Hourly weather ETL pipeline — OpenWeatherMap to Cloud SQL",
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["etl", "weather"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_weather",
        python_callable=extract,
        provide_context=True,
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load,
        provide_context=True,
    )

    extract_task >> transform_task >> load_task