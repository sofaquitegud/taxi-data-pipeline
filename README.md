# NYC Taxi Data Pipeline

[![Python](https://img.shields.io/badge/Python-3.11-blue.svg)](https://www.python.org/)
[![Airflow](https://img.shields.io/badge/Airflow-2.8.1-orange.svg)](https://airflow.apache.org/)
[![Spark](https://img.shields.io/badge/Spark-3.5.0-red.svg)](https://spark.apache.org/)
[![dbt](https://img.shields.io/badge/dbt-1.7.4-orange.svg)](https://www.getdbt.com/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

An end-to-end data engineering portfolio project demonstrating modern data pipeline architecture, orchestration, and analytics using NYC Taxi & Limousine Commission trip data.

## 🎯 Project Overview

This project showcases production-ready data engineering skills including:
- **Data Ingestion**: Automated download and validation of public datasets
- **Data Processing**: Distributed processing with PySpark
- **Orchestration**: Workflow management with Apache Airflow
- **Data Modeling**: Dimensional modeling with dbt
- **Data Quality**: Automated validation with Great Expectations
- **Analytics**: Interactive dashboards with Metabase
- **Infrastructure**: Containerized services with Docker

## 🏗️ Architecture

```
NYC TLC API → Ingestion → Raw Data Lake (MinIO) → Transformation (Spark) 
→ Processed Data Lake → dbt Models → Data Warehouse (PostgreSQL) 
→ Analytics (Metabase)
```

## 🛠️ Technology Stack

| Component | Technology |
|-----------|------------|
| Orchestration | Apache Airflow |
| Processing | PySpark |
| Storage (Lake) | MinIO (S3-compatible) |
| Storage (Warehouse) | PostgreSQL |
| Transformation | dbt Core |
| Data Quality | Great Expectations |
| Visualization | Metabase |
| Containerization | Docker & Docker Compose |

## 📋 Prerequisites

- Docker Desktop (with Docker Compose)
- 8GB RAM minimum (16GB recommended)
- 20GB free disk space
- Git

## 🚀 Quick Start

1. **Clone the repository**
   ```bash
   git clone <your-repo-url>
   cd taxi-data-pipeline
   ```

2. **Initial setup**
   ```bash
   make setup
   ```

3. **Start all services**
   ```bash
   make start
   ```

4. **Access the services**
   - Airflow UI: http://localhost:8080 (username: `airflow`, password: `airflow`)
   - MinIO Console: http://localhost:9001 (username: `minioadmin`, password: `minioadmin`)
   - Metabase: http://localhost:3000

## 📁 Project Structure

```
taxi-data-pipeline/
├── airflow/              # Airflow DAGs and configuration
├── dbt/                  # dbt models and tests
├── spark/                # PySpark jobs
├── ingestion/            # Data ingestion scripts
├── great_expectations/   # Data quality expectations
├── infra/                # Docker and init scripts
├── tests/                # Unit and integration tests
├── notebooks/            # Jupyter notebooks for exploration
└── docs/                 # Documentation
```

## 📊 Data Model

The project implements a star schema with:
- **Fact Table**: `fact_trips`
- **Dimension Tables**: `dim_datetime`, `dim_location`, `dim_payment`, `dim_rate`
- **Aggregate Tables**: Daily summaries, hourly patterns, zone performance

## 🧪 Testing

```bash
make test
```

## 📝 Documentation

See the [docs](./docs) directory for detailed documentation:
- [Architecture](./docs/architecture.md)
- [Setup Guide](./docs/setup_guide.md)
- [Data Dictionary](./docs/data_dictionary.md)

## 🤝 Contributing

This is a portfolio project, but suggestions are welcome! Please open an issue first to discuss proposed changes.

## 📄 License

This project is licensed under the MIT License.

## 🙏 Acknowledgments

- NYC Taxi & Limousine Commission for providing the data
- Open-source community for the amazing tools

---

**Built with ❤️ for learning and showcasing data engineering skills**
