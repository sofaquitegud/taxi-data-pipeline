"""Load processed data from MinIO into PostgreSQL for dbt."""

import os
import sys
import logging
from io import BytesIO

import pandas as pd
import psycopg2
from psycopg2 import sql
from minio import Minio

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DataLoader:
    """Load data from MinIO to PostgreSQL."""

    def __init__(self):
        """Initialize connections."""
        # MinIO config
        self.minio_client = Minio(
            os.getenv("MINIO_ENDPOINT", "minio:9000"),
            access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            secure=False,
        )

        # PostgreSQL config
        self.pg_conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            database="taxi_warehouse",
            user=os.getenv("WAREHOUSE_USER", "dataeng"),
            password=os.getenv("WAREHOUSE_PASSWORD", "dataeng123"),
        )
        self.pg_conn.autocommit = True

    def list_parquet_files(self, bucket: str, prefix: str) -> list:
        """List all parquet files in MinIO bucket with prefix."""
        objects = self.minio_client.list_objects(bucket, prefix=prefix, recursive=True)
        return [
            obj.object_name for obj in objects if obj.object_name.endswith(".parquet")
        ]

    def read_parquet_from_minio(self, bucket: str, object_name: str) -> pd.DataFrame:
        """Read parquet file from MinIO into DataFrame."""
        response = self.minio_client.get_object(bucket, object_name)
        data = response.read()
        response.close()
        response.release_conn()

        df = pd.read_parquet(BytesIO(data))
        return df

    def create_table_from_df(self, df: pd.DataFrame, schema: str, table_name: str):
        """Create table in PostgreSQL from DataFrame schema."""
        # Map pandas dtypes to PostgreSQL types
        type_mapping = {
            "int64": "BIGINT",
            "int32": "INTEGER",
            "float64": "DOUBLE PRECISION",
            "float32": "REAL",
            "bool": "BOOLEAN",
            "datetime64[ns]": "TIMESTAMP",
            "datetime64[us]": "TIMESTAMP",
            "object": "TEXT",
        }

        columns = []
        for col, dtype in df.dtypes.items():
            pg_type = type_mapping.get(str(dtype), "TEXT")
            columns.append(f'"{col}" {pg_type}')

        create_sql = f"""
            DROP TABLE IF EXISTS {schema}.{table_name};
            CREATE TABLE {schema}.{table_name} (
                {', '.join(columns)}
            );
        """

        with self.pg_conn.cursor() as cur:
            cur.execute(create_sql)

        logger.info(f"Created table {schema}.{table_name}")

    def load_df_to_postgres(self, df: pd.DataFrame, schema: str, table_name: str):
        """Load DataFrame to PostgreSQL table."""
        from io import StringIO

        # Create a buffer
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False, sep="\t", na_rep="\\N")
        buffer.seek(0)

        with self.pg_conn.cursor() as cur:
            cur.copy_expert(
                f"COPY {schema}.{table_name} FROM STDIN WITH CSV DELIMITER E'\\t' NULL '\\N'",
                buffer,
            )

        logger.info(f"Loaded {len(df):,} rows to {schema}.{table_name}")

    def load_taxi_data(self, taxi_type: str, year: int, month: int):
        """Load taxi data from MinIO to PostgreSQL."""
        bucket = "taxi-processed"
        prefix = f"{taxi_type}/year={year}/month={month:02d}/"

        logger.info(f"Loading {taxi_type} data for {year}-{month:02d}")

        # List all parquet files
        files = self.list_parquet_files(bucket, prefix)

        if not files:
            logger.warning(f"No files found for {prefix}")
            return 0

        # Read and combine all parquet files
        dfs = []
        for file in files:
            logger.info(f"Reading {file}")
            df = self.read_parquet_from_minio(bucket, file)
            dfs.append(df)

        combined_df = pd.concat(dfs, ignore_index=True)
        logger.info(f"Combined {len(combined_df):,} rows from {len(files)} files")

        # Determine table name
        table_name = f"{taxi_type}_taxi_trips"
        schema = "staging"

        # Create table and load data
        self.create_table_from_df(combined_df, schema, table_name)
        self.load_df_to_postgres(combined_df, schema, table_name)

        return len(combined_df)

    def close(self):
        """Close connections."""
        if self.pg_conn:
            self.pg_conn.close()


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Load data from MinIO to PostgreSQL")
    parser.add_argument("--taxi-type", default="yellow", choices=["yellow", "green"])
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)

    args = parser.parse_args()

    loader = DataLoader()

    try:
        rows = loader.load_taxi_data(
            taxi_type=args.taxi_type, year=args.year, month=args.month
        )

        logger.info("=" * 50)
        logger.info(f"Successfully loaded {rows:,} rows to PostgreSQL")

    finally:
        loader.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
