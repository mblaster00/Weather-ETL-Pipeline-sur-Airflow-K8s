<div align="center">

<img src="https://img.shields.io/badge/Apache%20Airflow-2.11.0-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white"/>
<img src="https://img.shields.io/badge/Kubernetes-GKE-326CE5?style=for-the-badge&logo=kubernetes&logoColor=white"/>
<img src="https://img.shields.io/badge/Terraform-1.5+-7B42BC?style=for-the-badge&logo=terraform&logoColor=white"/>
<img src="https://img.shields.io/badge/PostgreSQL-Cloud%20SQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white"/>
<img src="https://img.shields.io/badge/Python-3.11-3776AB?style=for-the-badge&logo=python&logoColor=white"/>

<br/><br/>

# weather-etl-pipeline-sur-airflow-k8s

### Production-grade ETL pipeline orchestrated by Apache Airflow,
### deployed on Google Kubernetes Engine via Terraform & Helm.

<br/>

[Overview](#-overview) · [Architecture](#-architecture) · [Pipeline](#-pipeline) · [Security](#-security) · [Getting Started](#-getting-started) · [Project Structure](#-project-structure) · [Monitoring](#-monitoring) · [Lessons Learned](#-lessons-learned) · [Roadmap](#-roadmap)

</div>

---

## Overview

This project implements a cloud-native, fault-tolerant ETL pipeline that ingests hourly weather data from the [OpenWeatherMap API](https://openweathermap.org/api), transforms it, and persists it to a managed PostgreSQL database on GCP.

Built as a hands-on deep dive into Data Engineering fundamentals with a focus on production patterns: isolated task execution, declarative infrastructure, zero-secret deployments via Workload Identity, and GitOps-style DAG delivery.

What makes this non-trivial:

- Infrastructure is fully reproducible from a single `terraform apply`
- DAGs sync automatically from GitHub via gitSync no redeploy needed to ship pipeline changes
- All secrets managed centrally via Google Secret Manager no `.env` files, no secrets in Git, full audit trail
- Cloud SQL access via Workload Identity no static credentials anywhere in the cluster
- Each ETL task runs in its own ephemeral Kubernetes pod (`KubernetesPodOperator`) in production mode

---

## Pipeline

The `weather_etl` DAG runs every hour with built-in resilience:

```
extract_weather ──► transform_data ──► load_to_postgres
 (PythonOperator)   (PythonOperator)   (PythonOperator)
```

| Task | Operator | Responsibility |
|---|---|---|
| `extract_weather` | `PythonOperator` | Fetches raw JSON from OpenWeatherMap API |
| `transform_data` | `PythonOperator` | Normalises and validates data via XComs |
| `load_to_postgres` | `PythonOperator` | Upserts records via `PostgresHook` |

Resilience config:

```python
default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}
```

Key Airflow concepts covered: DAGs · Operators · Hooks · XComs · Connections · Variables · LocalExecutor · gitSync

---

## Security

Secret management follows a zero-trust approach no credentials in the codebase, no shared `.env` files.

**Google Secret Manager** is the single source of truth for all secret values. Access is controlled per team member via IAM roles with a full audit trail of who accessed what and when.

```bash
# Create a secret done once by the team lead
echo -n "YOUR_VALUE" | gcloud secrets create openweather-api-key --data-file=-

# Grant access to a teammate
gcloud secrets add-iam-policy-binding openweather-api-key \
  --member="user:teammate@company.com" \
  --role="roles/secretmanager.secretAccessor"
```

**Kubernetes Secrets** are injected automatically by `inject-secrets.sh` pulled from Secret Manager, never stored on disk.

**Workload Identity** handles Cloud SQL authentication in production pods connect to the database without any password. GCP manages authentication transparently via Kubernetes and GCP Service Account bindings.

**Private networking** Cloud SQL has no public IP. All traffic stays within the VPC.

**gitSync SSH Deploy Key** DAG sync uses a read-only SSH deploy key with no passphrase. The private key lives in a Kubernetes Secret only, never committed to the repo.

---

## Getting Started

### Prerequisites

| Tool | Version | Install |
|---|---|---|
| Google Cloud SDK | latest | [cloud.google.com/sdk](https://cloud.google.com/sdk/docs/install) |
| Terraform | >= 1.5 | [developer.hashicorp.com/terraform](https://developer.hashicorp.com/terraform/install) |
| Helm | >= 3.12 | [helm.sh](https://helm.sh/docs/intro/install/) |
| kubectl | latest | [kubernetes.io](https://kubernetes.io/docs/tasks/tools/) |
| Docker | latest | [docs.docker.com](https://docs.docker.com/get-docker/) |

You will also need a GCP project with billing enabled and an [OpenWeatherMap API key](https://openweathermap.org/api) (free tier).

---

### 1 Configure Terraform

```bash
cd terraform/
cp terraform.tfvars.example terraform.tfvars
```

Fill in your `project_id` in `terraform.tfvars`.

---

### 2 Provision GKE with Terraform

```bash
terraform init
terraform plan
terraform apply
```

Connect kubectl to the cluster using the command printed in Terraform outputs:

```bash
gcloud container clusters get-credentials weather-etl-cluster \
  --region europe-west1 \
  --project YOUR_PROJECT_ID
```

---

### 3 Push secrets to Google Secret Manager

```bash
echo -n "YOUR_OPENWEATHER_API_KEY" | gcloud secrets create openweather-api-key --data-file=-
echo -n "YOUR_CLOUDSQL_PRIVATE_IP" | gcloud secrets create cloudsql-host --data-file=-
echo -n "weather" | gcloud secrets create cloudsql-database --data-file=-
echo -n "YOUR_WEBSERVER_PASSWORD" | gcloud secrets create airflow-webserver-password --data-file=-
echo -n "YOUR_POSTGRES_PASSWORD" | gcloud secrets create airflow-postgres-password --data-file=-
echo -n "YOUR_WEBSERVER_SECRET_KEY" | gcloud secrets create airflow-webserver-secret-key --data-file=-
echo -n "postgresql://postgres:YOUR_POSTGRES_PASSWORD@airflow-postgresql.airflow:5432/airflow" | gcloud secrets create airflow-metadata-connection --data-file=-
```

---

### 4 Generate and register the gitSync SSH key

```bash
ssh-keygen -t ed25519 -C "airflow-gitsync" -f ./airflow-gitsync-key -N ""
cat airflow-gitsync-key.pub
```

Add the public key to GitHub → repo Settings → Deploy keys → read-only.

```bash
kubectl create secret generic airflow-gitsync-ssh \
  --from-file=gitSshKey=./airflow-gitsync-key \
  --namespace airflow
rm ./airflow-gitsync-key ./airflow-gitsync-key.pub
```

---

### 5 Inject secrets into Kubernetes

```bash
chmod +x k8s/inject-secrets.sh
./k8s/inject-secrets.sh
```

---

### 6 Deploy Airflow via Helm

```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update

helm install airflow apache-airflow/airflow \
  --namespace airflow \
  --values helm/values.yaml \
  --timeout 10m
```

---

### 7 Run database migrations manually

```bash
kubectl run airflow-migrations \
  --image=apache/airflow:2.11.0 \
  --restart=Never \
  --namespace=airflow \
  --env="AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=$(kubectl get secret airflow-metadata-secret -n airflow -o jsonpath='{.data.connection}' | base64 --decode)" \
  -- airflow db migrate
```

---

### 8 Initialize the weather_data table

```bash
kubectl run psql-client \
  --image=postgres:15 \
  --restart=Never \
  --namespace=airflow \
  --env="PGPASSWORD=$(kubectl get secret airflow-postgres-secret -n airflow -o jsonpath='{.data.password}' | base64 --decode)" \
  -it --rm \
  -- psql -h airflow-postgresql.airflow -U postgres -d airflow
```

Run `init.sql` then exit with `\q`.

---

### 9 Set Airflow Variable and Connection

```bash
kubectl exec -it airflow-scheduler-0 -n airflow -c scheduler -- \
  airflow variables set openweather_api_key YOUR_API_KEY

kubectl exec -it airflow-scheduler-0 -n airflow -c scheduler -- \
  airflow connections add postgres_weather \
  --conn-type postgres \
  --conn-host airflow-postgresql.airflow \
  --conn-schema airflow \
  --conn-login postgres \
  --conn-password YOUR_POSTGRES_PASSWORD \
  --conn-port 5432
```

---

### 10 Access the UI and trigger the DAG

```bash
kubectl port-forward svc/airflow-webserver 8080:8080 --namespace airflow
```

Open `http://localhost:8080`, unpause the `weather_etl` DAG and trigger a first run.

---

## Views

### DAG graph with all tasks successful
![DAG graph view](screenshots/dag-graph.png)

### GKE Interface
![GKE workloads](screenshots/gke-interface.png)

### Secret Manager
![Secret Manager](screenshots/secret-manager.png)

### Artifact Registry
![Artifact Registry](screenshots/artifact-registry.png)

### Cloud SQL
![Cloud SQL](screenshots/cloud-sql.png)

---

## Project Structure

```
weather-etl-pipeline-sur-airflow-k8s/
│
├── terraform/                        # GCP infrastructure as code
│   ├── main.tf                       # VPC · GKE · Cloud SQL · Workload Identity
│   ├── variables.tf
│   ├── outputs.tf
│   └── terraform.tfvars.example
│
├── helm/
│   └── values.yaml                   # Airflow Helm overrides no secrets inside
│
├── dags/
│   ├── weather_etl.py                # Main DAG
│   ├── init_db.py                    # One-time schema initialisation DAG
│   ├── sql/
│   │   └── init.sql                  # Table definition + indexes
│   └── utils/
│       ├── extract.py                # OpenWeatherMap API client
│       ├── transform.py              # Data normalisation
│       └── load.py                   # PostgreSQL upsert
│
├── docker/
│   ├── Dockerfile                    # Image for ETL pods
│   └── requirements.txt
│
├── k8s/
│   ├── namespace.yaml
│   └── inject-secrets.sh             # Pulls from Secret Manager → K8s Secrets
│
├── .gitignore
└── README.md
```

---

## Monitoring

| Layer | Tool | What it covers |
|---|---|---|
| Pipeline | Airflow UI | DAG runs · task states · logs per task |
| Infrastructure | Cloud Monitoring | Pod CPU/memory · node health · Cloud SQL metrics |
| Alerts | Cloud Alerting | Task failure notifications · SLA misses |
| Logs | Cloud Logging | Centralised logs from all pods |
| Secret access | Cloud Audit Logs | Who accessed which secret and when |

---

## Roadmap

- [x] Project design & architecture
- [x] Terraform: VPC + GKE + Cloud SQL + Workload Identity
- [x] Helm: Airflow deployment + gitSync
- [x] Secret management via Google Secret Manager
- [x] First DAG with `PythonOperator` end to end successful run
- [x] Workload Identity + GCP Service Account for ETL pods
- [x] Docker image for ETL pods pushed to Artifact Registry
- [ ] Refactor to `KubernetesPodOperator` on a production-sized cluster
- [ ] CI/CD pipeline auto build and push image on merge to main
- [ ] Grafana dashboard on collected weather data
- [ ] Multi-city support with dynamic task generation

---

## References

- [Apache Airflow Core Concepts](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/index.html)
- [KubernetesPodOperator](https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/operators.html)
- [Airflow Helm Chart](https://airflow.apache.org/docs/helm-chart/stable/index.html)
- [GKE Workload Identity](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity)
- [Google Secret Manager](https://cloud.google.com/secret-manager/docs/quickstart)
- [Terraform GCP Provider](https://registry.terraform.io/providers/hashicorp/google/latest/docs)
- [External Secrets Operator](https://external-secrets.io/latest/)
- [Alembic database migrations](https://alembic.sqlalchemy.org/en/latest/)