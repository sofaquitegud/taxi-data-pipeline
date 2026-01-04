# Setup Guide

## Prerequisites

- **Docker Desktop** with Docker Compose
- **8GB RAM** minimum (16GB recommended)
- **20GB free disk space**
- **Git**

## Quick Start

### 1. Clone the Repository

```bash
git clone <your-repo-url>
cd taxi-data-pipeline
```

### 2. Create Environment File

```bash
cp .env.example .env
```

### 3. Start Services

```bash
make setup
make start
```

### 4. Verify Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | airflow / airflow |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Metabase | http://localhost:3000 | Set up on first login |

## Running the Pipeline

### Step 1: Ingest Data

```bash
docker compose exec airflow-webserver bash
cd /opt/ingestion
python download_tlc_data.py --taxi-type yellow --start-year 2023 --start-month 1 --end-year 2023 --end-month 1
```

### Step 2: Transform Data

```bash
cd /opt/spark/jobs
python clean_taxi_data.py --taxi-type yellow --year 2023 --month 1
python feature_engineering.py --taxi-type yellow --year 2023 --month 1
```

### Step 3: Run dbt Models

```bash
cd /opt/dbt
dbt seed
dbt run
dbt test
```

## Stopping Services

```bash
make stop
```

## Cleanup

Remove all containers and volumes:
```bash
make clean
```

## Troubleshooting

### Airflow containers unhealthy
Wait 2-3 minutes for initialization to complete.

### Permission errors
Ensure `AIRFLOW_UID` in `.env` matches your user ID (`id -u`).

### MinIO connection issues
Verify MinIO is running: `docker compose ps`
