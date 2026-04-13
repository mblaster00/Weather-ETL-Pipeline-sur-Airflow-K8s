#!/bin/bash

PROJECT_ID="weather-etl-airflow"

echo "Creating namespace..."
kubectl create namespace airflow --dry-run=client -o yaml | kubectl apply -f -

echo "Pulling secrets from Secret Manager..."
API_KEY=$(gcloud secrets versions access latest --secret="openweather-api-key" --project=$PROJECT_ID)
WEBSERVER_SECRET_KEY=$(gcloud secrets versions access latest --secret="airflow-webserver-secret-key" --project=$PROJECT_ID)
METADATA_CONN=$(gcloud secrets versions access latest --secret="airflow-metadata-connection" --project=$PROJECT_ID)
CLOUDSQL_HOST=$(gcloud secrets versions access latest --secret="cloudsql-host" --project=$PROJECT_ID)
CLOUDSQL_DB=$(gcloud secrets versions access latest --secret="cloudsql-database" --project=$PROJECT_ID)
WEBSERVER_PASSWORD=$(gcloud secrets versions access latest --secret="airflow-webserver-password" --project=$PROJECT_ID)
POSTGRES_PASSWORD=$(gcloud secrets versions access latest --secret="airflow-postgres-password" --project=$PROJECT_ID)
CLOUDSQL_PASSWORD=$(gcloud secrets versions access latest --secret="cloudsql-postgres-password" --project=$PROJECT_ID)
echo "All secrets pulled successfully."

echo "Injecting into Kubernetes Secrets..."

kubectl create secret generic cloudsql-postgres-secret \
  --from-literal=password=$CLOUDSQL_PASSWORD \
  --namespace airflow \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl create secret generic weather-api-secret \
  --from-literal=api_key=$API_KEY \
  --namespace airflow \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl create secret generic cloudsql-secret \
  --from-literal=host=$CLOUDSQL_HOST \
  --from-literal=database=$CLOUDSQL_DB \
  --namespace airflow \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl create secret generic airflow-webserver-secret \
  --from-literal=password=$WEBSERVER_PASSWORD \
  --namespace airflow \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl create secret generic airflow-postgres-secret \
  --from-literal=password=$POSTGRES_PASSWORD \
  --namespace airflow \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl create secret generic airflow-webserver-secret-key \
  --from-literal=webserver-secret-key=$WEBSERVER_SECRET_KEY \
  --namespace airflow \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl create secret generic airflow-metadata-secret \
  --from-literal=connection=$METADATA_CONN \
  --namespace airflow \
  --dry-run=client -o yaml | kubectl apply -f -

echo "Done. All secrets injected."