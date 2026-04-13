import os
import json
import psycopg2
from google.cloud import storage


def load():
    client = storage.Client()
    bucket = client.bucket("weather-etl-data")

    # Read transformed data from Cloud Storage
    data = json.loads(
        bucket.blob("transformed/latest.json").download_as_text()
    )

    # Connect to Cloud SQL via Workload Identity
    conn = psycopg2.connect(
        host=os.environ["CLOUDSQL_HOST"],
        database=os.environ["CLOUDSQL_DATABASE"],
        user="postgres",
        password=os.environ["CLOUDSQL_PASSWORD"],
    )

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO weather_data
                (city, temperature, humidity, description, recorded_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (city, recorded_at) DO NOTHING
        """, (
            data["city"],
            data["temperature"],
            data["humidity"],
            data["description"],
            data["recorded_at"],
        ))

    conn.commit()
    conn.close()

    print(f"Loaded data for {data['city']} into PostgreSQL successfully.")


if __name__ == "__main__":
    load()