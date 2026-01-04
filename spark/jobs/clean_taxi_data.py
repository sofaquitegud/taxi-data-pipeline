"""Clean and transform raw taxi data."""

import logging
import sys
import os
from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    TimestampType,
    StringType,
)

# Add parent directory to path
sys.path.insert(0, "/opt/spark/utils")
from spark_session import get_spark_session, stop_spark_session
from data_validators import DataValidator

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class TaxiDataCleaner:
    """Clean and transform taxi trip data."""

    # Outlier thresholds
    MAX_TRIP_DISTANCE = 500  # miles
    MAX_FARE_AMOUNT = 1000  # dollars
    MAX_TOTAL_AMOUNT = 2000  # dollars
    MIN_PASSENGER_COUNT = 0
    MAX_PASSENGER_COUNT = 10
    MAX_TRIP_DURATION_HOURS = 24

    def __init__(self, spark):
        """Initialize with Spark session."""
        self.spark = spark

    def read_raw_data(self, taxi_type: str, year: int, month: int) -> DataFrame:
        """Read raw data from MinIO."""
        path = f"s3a://taxi-raw/{taxi_type}/year={year}/month={month:02d}/*.parquet"
        logger.info(f"Reading from: {path}")

        df = self.spark.read.parquet(path)
        logger.info(f"Read {df.count():,} rows")
        return df

    def normalize_column_names(self, df: DataFrame, taxi_type: str) -> DataFrame:
        """Normalize column names across taxi types."""
        # Rename pickup/dropoff datetime columns
        if taxi_type == "yellow":
            df = df.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
            df = df.withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
        elif taxi_type == "green":
            df = df.withColumnRenamed("lpep_pickup_datetime", "pickup_datetime")
            df = df.withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")

        # Add taxi type column
        df = df.withColumn("taxi_type", F.lit(taxi_type))

        return df

    def add_derived_columns(self, df: DataFrame) -> DataFrame:
        """Add computed/derived columns."""
        # Use unix_timestamp for TIMESTAMP_NTZ compatibility
        df = df.withColumn(
            "trip_duration_minutes",
            (
                F.unix_timestamp(F.col("dropoff_datetime"))
                - F.unix_timestamp(F.col("pickup_datetime"))
            )
            / 60,
        )

        df = df.withColumn("trip_duration_hours", F.col("trip_duration_minutes") / 60)

        # Speed calculation (avoid division by zero)
        df = df.withColumn(
            "avg_speed_mph",
            F.when(
                F.col("trip_duration_hours") > 0,
                F.col("trip_distance") / F.col("trip_duration_hours"),
            ).otherwise(0),
        )

        # Time-based features
        df = df.withColumn("pickup_hour", F.hour("pickup_datetime"))
        df = df.withColumn("pickup_day_of_week", F.dayofweek("pickup_datetime"))
        df = df.withColumn("pickup_month", F.month("pickup_datetime"))
        df = df.withColumn("pickup_year", F.year("pickup_datetime"))

        # Is weekend
        df = df.withColumn(
            "is_weekend",
            F.when(F.col("pickup_day_of_week").isin([1, 7]), True).otherwise(False),
        )

        # Time of day category
        df = df.withColumn(
            "time_of_day",
            F.when(F.col("pickup_hour").between(6, 11), "morning")
            .when(F.col("pickup_hour").between(12, 17), "afternoon")
            .when(F.col("pickup_hour").between(18, 21), "evening")
            .otherwise("night"),
        )

        return df

    def remove_outliers(self, df: DataFrame) -> DataFrame:
        """Remove outlier records."""
        original_count = df.count()

        # Filter conditions
        df_cleaned = df.filter(
            # Valid trip distance
            (F.col("trip_distance") >= 0)
            & (F.col("trip_distance") <= self.MAX_TRIP_DISTANCE)
            &
            # Valid fare
            (F.col("fare_amount") >= 0)
            & (F.col("fare_amount") <= self.MAX_FARE_AMOUNT)
            &
            # Valid total amount
            (F.col("total_amount") >= 0)
            & (F.col("total_amount") <= self.MAX_TOTAL_AMOUNT)
            &
            # Valid passenger count
            (F.col("passenger_count") >= self.MIN_PASSENGER_COUNT)
            & (F.col("passenger_count") <= self.MAX_PASSENGER_COUNT)
            &
            # Valid trip duration
            (F.col("trip_duration_hours") >= 0)
            & (F.col("trip_duration_hours") <= self.MAX_TRIP_DURATION_HOURS)
            &
            # Valid dates (pickup before dropoff)
            (F.col("dropoff_datetime") > F.col("pickup_datetime"))
        )

        cleaned_count = df_cleaned.count()
        removed = original_count - cleaned_count
        logger.info(
            f"Removed {removed:,} outlier records ({removed/original_count:.2%})"
        )

        return df_cleaned

    def handle_nulls(self, df: DataFrame) -> DataFrame:
        """Handle null values with appropriate defaults."""
        # Fill numeric nulls with 0
        numeric_cols = ["passenger_count", "congestion_surcharge", "Airport_fee"]
        for col in numeric_cols:
            if col in df.columns:
                df = df.fillna({col: 0})

        # Fill string nulls with 'N'
        if "store_and_fwd_flag" in df.columns:
            df = df.fillna({"store_and_fwd_flag": "N"})

        return df

    def select_final_columns(self, df: DataFrame) -> DataFrame:
        """Select and order final columns."""
        columns = [
            "taxi_type",
            "pickup_datetime",
            "dropoff_datetime",
            "pickup_year",
            "pickup_month",
            "pickup_hour",
            "pickup_day_of_week",
            "is_weekend",
            "time_of_day",
            "passenger_count",
            "trip_distance",
            "trip_duration_minutes",
            "avg_speed_mph",
            "PULocationID",
            "DOLocationID",
            "RatecodeID",
            "payment_type",
            "fare_amount",
            "extra",
            "mta_tax",
            "tip_amount",
            "tolls_amount",
            "total_amount",
        ]

        # Only select columns that exist
        available_cols = [c for c in columns if c in df.columns]
        return df.select(available_cols)

    def write_processed_data(
        self, df: DataFrame, taxi_type: str, year: int, month: int
    ) -> str:
        """Write processed data to MinIO."""
        output_path = f"s3a://taxi-processed/{taxi_type}/year={year}/month={month:02d}"

        logger.info(f"Writing to: {output_path}")

        df.write.mode("overwrite").partitionBy("pickup_day_of_week").parquet(
            output_path
        )

        logger.info(f"Successfully wrote {df.count():,} rows")
        return output_path

    def process(self, taxi_type: str, year: int, month: int) -> dict:
        """Run the full cleaning pipeline."""
        logger.info(f"Processing {taxi_type} taxi data for {year}-{month:02d}")

        # Read raw data
        df = self.read_raw_data(taxi_type, year, month)
        initial_count = df.count()

        # Transform
        df = self.normalize_column_names(df, taxi_type)
        df = self.add_derived_columns(df)
        df = self.handle_nulls(df)
        df = self.remove_outliers(df)
        df = self.select_final_columns(df)

        final_count = df.count()

        # Validate
        validator = DataValidator(df)
        passed, row_count = validator.check_row_count(min_rows=1000)

        if not passed:
            raise ValueError(f"Validation failed: only {row_count} rows")

        # Write
        output_path = self.write_processed_data(df, taxi_type, year, month)

        return {
            "taxi_type": taxi_type,
            "year": year,
            "month": month,
            "initial_rows": initial_count,
            "final_rows": final_count,
            "rows_removed": initial_count - final_count,
            "output_path": output_path,
        }


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Clean taxi data")
    parser.add_argument("--taxi-type", default="yellow", choices=["yellow", "green"])
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)

    args = parser.parse_args()

    spark = get_spark_session("TaxiDataCleaning")

    try:
        cleaner = TaxiDataCleaner(spark)
        result = cleaner.process(
            taxi_type=args.taxi_type, year=args.year, month=args.month
        )

        logger.info("=" * 50)
        logger.info("Processing complete!")
        logger.info(f"  Initial rows: {result['initial_rows']:,}")
        logger.info(f"  Final rows: {result['final_rows']:,}")
        logger.info(f"  Rows removed: {result['rows_removed']:,}")
        logger.info(f"  Output: {result['output_path']}")

    finally:
        stop_spark_session(spark)

    return 0


if __name__ == "__main__":
    sys.exit(main())
