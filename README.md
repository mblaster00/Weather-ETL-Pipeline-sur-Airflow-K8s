<div align="center">

<img src="https://img.shields.io/badge/Apache%20Airflow-2.9+-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white"/>
<img src="https://img.shields.io/badge/Kubernetes-GKE-326CE5?style=for-the-badge&logo=kubernetes&logoColor=white"/>
<img src="https://img.shields.io/badge/Terraform-1.5+-7B42BC?style=for-the-badge&logo=terraform&logoColor=white"/>
<img src="https://img.shields.io/badge/PostgreSQL-Cloud%20SQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white"/>
<img src="https://img.shields.io/badge/Python-3.11-3776AB?style=for-the-badge&logo=python&logoColor=white"/>

<br/><br/>

# weather-etl-airflow

### Production-grade ETL pipeline orchestrated by Apache Airflow,
### deployed on Google Kubernetes Engine via Terraform & Helm.

<br/>

[Overview](#-overview) · [Architecture](#-architecture) · [Pipeline](#-pipeline) · [Getting Started](#-getting-started) · [Project Structure](#-project-structure) · [Monitoring](#-monitoring) · [Roadmap](#-roadmap)

</div>

---

## 🎯 Overview

This project implements a **cloud-native, fault-tolerant ETL pipeline** that ingests hourly weather data from the [OpenWeatherMap API](https://openweathermap.org/api), transforms it, and persists it to a managed PostgreSQL database on GCP.

Built as a hands-on deep dive into **Data Engineering fundamentals** with a focus on production patterns: isolated task execution, declarative infrastructure, zero-secret deployments via Workload Identity, and GitOps-style DAG delivery.

**What makes this non-trivial:**

- Each ETL task runs in its own **ephemeral Kubernetes pod** (`KubernetesPodOperator`) full isolation, independent scaling, no shared state
- Infrastructure is **fully reproducible** from a single `terraform apply`
- DAGs sync automatically from GitHub **no redeploy needed** to ship pipeline changes
- Cloud SQL access via **Workload Identity** no static credentials anywhere in the cluster

---

## ⚙️ Pipeline

The `weather_etl` DAG runs **every hour** with built-in resilience:

```
check_api_health ──► extract_weather ──► transform_data ──► load_to_postgres
   (HttpSensor)    (KubernetesPod)     (KubernetesPod)     (KubernetesPod)
```

| Task | Operator | Responsibility |
|---|---|---|
| `check_api_health` | `HttpSensor` | Guards the pipeline — skips execution if API is unreachable |
| `extract_weather` | `KubernetesPodOperator` | Fetches raw JSON from OpenWeatherMap for N cities |
| `transform_data` | `KubernetesPodOperator` | Validates schema, normalises units, deduplicates |
| `load_to_postgres` | `KubernetesPodOperator` | Upserts records via `PostgresHook`, logs row counts |

**Resilience config:**

```python
default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": notify_on_failure,
}
```

**Key Airflow concepts covered:** DAGs · Operators · Sensors · Hooks · XComs · Connections · Variables · task isolation with `KubernetesExecutor`

---

## 🚀 Getting Started

### Prerequisites

| Tool | Version | Install |
|---|---|---|
| Google Cloud SDK | latest | [cloud.google.com/sdk](https://cloud.google.com/sdk/docs/install) |
| Terraform | >= 1.5 | [developer.hashicorp.com/terraform](https://developer.hashicorp.com/terraform/install) |
| Helm | >= 3.12 | [helm.sh](https://helm.sh/docs/intro/install/) |
| kubectl | latest | [kubernetes.io](https://kubernetes.io/docs/tasks/tools/) |

You'll also need a GCP project with billing enabled and an [OpenWeatherMap API key](https://openweathermap.org/api) (free tier).

---

### 1 — Provision GKE with Terraform

```bash
cd terraform/

terraform init
terraform plan -var="project_id=<YOUR_GCP_PROJECT>"
terraform apply -var="project_id=<YOUR_GCP_PROJECT>"
```

This provisions:
- GKE Autopilot cluster in `europe-west1`
- Cloud SQL PostgreSQL 15 instance
- VPC + private networking
- Workload Identity bindings (no static DB credentials needed)
- Kubernetes namespaces: `airflow`, `data`

Connect `kubectl` to the new cluster:

```bash
gcloud container clusters get-credentials weather-etl-cluster \
  --region europe-west1 \
  --project <YOUR_GCP_PROJECT>
```

---

### 2 — Deploy Airflow via Helm

```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update

helm install airflow apache-airflow/airflow \
  --namespace airflow \
  --values helm/values.yaml
```

`helm/values.yaml` configures:
- `executor: KubernetesExecutor`
- `gitSync` pointing to this repo (DAGs delivered automatically)
- Resource requests/limits per pod
- Cloud SQL connection string

Verify the rollout:

```bash
kubectl rollout status deployment/airflow-webserver -n airflow
kubectl get pods -n airflow
```

---

### 3 — Inject Secrets

```bash
# OpenWeatherMap API key
kubectl create secret generic weather-api-secret \
  --from-literal=api_key=<YOUR_API_KEY> \
  --namespace airflow

# Cloud SQL access is handled by Workload Identity — no password needed
```

---

### 4 — Access the UI

```bash
kubectl port-forward svc/airflow-webserver 8080:8080 -n airflow
open http://localhost:8080
```

Enable the `weather_etl` DAG and trigger a first run manually to validate the full pipeline.

---

## 📊 Monitoring

| Layer | Tool | What it covers |
|---|---|---|
| Pipeline | Airflow UI | DAG runs, task states, XCom values, logs per pod |
| Infrastructure | Cloud Monitoring | Pod CPU/memory, node health, Cloud SQL metrics |
| Alerts | Cloud Alerting | Task failure notifications, SLA misses |
| Logs | Cloud Logging | Centralised logs from all ephemeral pods |

---

## 📚 References

- [Apache Airflow — Core Concepts](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/index.html)
- [KubernetesPodOperator](https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/operators.html)
- [Airflow Helm Chart](https://airflow.apache.org/docs/helm-chart/stable/index.html)
- [GKE Workload Identity](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity)
- [Terraform GCP Provider](https://registry.terraform.io/providers/hashicorp/google/latest/docs)

---

<div align="center">

Designed for production.

</div>
