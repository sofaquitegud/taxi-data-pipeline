# Architecture

## Overview

This project implements a modern data lakehouse architecture for processing NYC taxi trip data.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           NYC Taxi Data Pipeline                            │
└─────────────────────────────────────────────────────────────────────────────┘

┌──────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  NYC TLC     │───▶│  Ingestion  │───▶│   MinIO     │───▶│   Spark     │
│  Data Source │    │   Scripts   │    │ (Raw Layer) │    │  Processing │
└──────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
                                                               │
                    ┌─────────────┐    ┌─────────────┐         │
                    │  Metabase   │◀───│  PostgreSQL │◀────────┘
                    │ (Analytics) │    │ (Warehouse) │
                    └─────────────┘    └─────────────┘
                                            ▲
                                            │
                                       ┌────┴────┐
                                       │   dbt   │
                                       │ Models  │
                                       └─────────┘
```

## Data Flow

### 1. Ingestion Layer
- **Source**: NYC TLC public dataset (Parquet files)
- **Tool**: Python scripts with validation
- **Destination**: MinIO `taxi-raw` bucket
- **Partitioning**: `taxi_type/year=YYYY/month=MM/`

### 2. Processing Layer
- **Tool**: PySpark
- **Operations**: Cleaning, validation, feature engineering
- **Output**: MinIO `taxi-processed` bucket

### 3. Transformation Layer
- **Tool**: dbt Core
- **Models**: Staging → Core (Dims/Facts) → Analytics
- **Destination**: PostgreSQL data warehouse

### 4. Analytics Layer
- **Tool**: Metabase
- **Features**: Interactive dashboards, SQL queries

## Orchestration

Apache Airflow manages the pipeline with these DAGs:
- `taxi_data_ingestion`: Monthly data download
- `taxi_data_transformation`: Spark cleaning jobs
- `taxi_dbt_pipeline`: dbt model execution

## Technology Choices

| Component | Choice | Rationale |
|-----------|--------|-----------|
| Orchestration | Airflow | Industry standard, DAG-based workflows |
| Processing | PySpark | Scalable, handles large datasets |
| Object Storage | MinIO | S3-compatible, runs locally |
| Warehouse | PostgreSQL | Reliable, SQL-compliant |
| Transformation | dbt | SQL-based, testable, documented |
| Visualization | Metabase | Open-source, easy to use |
