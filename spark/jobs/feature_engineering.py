"""Feature engineering for taxi data analytics."""

import logging
import sys
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.insert(0, "/opt/spark/utils")
from spark_session import get_spark_session, stop_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FeatureEngineer:
    """Create aggregated features from processed taxi data."""

    def __init__(self, spark):
        """Initialize with Spark session."""
        self.spark = spark

    def read_processed_data(
        self, taxi_type: str = None, year: int = None, month: int = None
    ) -> DataFrame:
        """Read processed data from MinIO."""
        if taxi_type and year and month:
            path = f"s3a://taxi-processed/{taxi_type}/year={year}/month={month:02d}/*/*.parquet"
        elif taxi_type:
            path = f"s3a://taxi-processed/{taxi_type}/*/*.parquet"
        else:
            path = "s3a://taxi-processed/*/*/*/*.parquet"

        logger.info(f"Reading from: {path}")
        return self.spark.read.parquet(path)

    def create_hourly_aggregates(self, df: DataFrame) -> DataFrame:
        """Create hourly demand aggregates."""
        # Derive day of week from pickup_datetime (partition column not in parquet)
        df = df.withColumn("pickup_day_of_week", F.dayofweek("pickup_datetime"))

        hourly_agg = df.groupBy(
            "taxi_type",
            "pickup_year",
            "pickup_month",
            "pickup_day_of_week",
            "pickup_hour",
        ).agg(
            F.count("*").alias("trip_count"),
            F.sum("passenger_count").alias("total_passengers"),
            F.avg("trip_distance").alias("avg_distance"),
            F.avg("trip_duration_minutes").alias("avg_duration"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("total_amount").alias("avg_total"),
            F.avg("tip_amount").alias("avg_tip"),
            F.sum("total_amount").alias("total_revenue"),
        )

        return hourly_agg

    def create_location_aggregates(self, df: DataFrame) -> DataFrame:
        """Create location-based aggregates."""
        pickup_agg = (
            df.groupBy("taxi_type", "pickup_year", "pickup_month", "PULocationID")
            .agg(
                F.count("*").alias("pickup_count"),
                F.avg("fare_amount").alias("avg_fare_from"),
                F.avg("trip_distance").alias("avg_distance_from"),
            )
            .withColumnRenamed("PULocationID", "location_id")
        )

        dropoff_agg = (
            df.groupBy("taxi_type", "pickup_year", "pickup_month", "DOLocationID")
            .agg(
                F.count("*").alias("dropoff_count"),
                F.avg("fare_amount").alias("avg_fare_to"),
                F.avg("trip_distance").alias("avg_distance_to"),
            )
            .withColumnRenamed("DOLocationID", "location_id")
        )

        # Join pickup and dropoff aggregates
        location_agg = pickup_agg.join(
            dropoff_agg,
            ["taxi_type", "pickup_year", "pickup_month", "location_id"],
            "outer",
        ).fillna(0)

        return location_agg

    def create_daily_summary(self, df: DataFrame) -> DataFrame:
        """Create daily summary statistics."""
        daily = df.withColumn("pickup_date", F.to_date("pickup_datetime"))

        daily_summary = daily.groupBy("taxi_type", "pickup_date").agg(
            F.count("*").alias("trip_count"),
            F.sum("passenger_count").alias("total_passengers"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("trip_distance").alias("avg_distance"),
            F.avg("trip_duration_minutes").alias("avg_duration"),
            F.avg("avg_speed_mph").alias("avg_speed"),
            F.percentile_approx("fare_amount", 0.5).alias("median_fare"),
            F.stddev("fare_amount").alias("fare_stddev"),
        )

        return daily_summary

    def write_aggregates(
        self, df: DataFrame, aggregate_type: str, year: int, month: int
    ) -> str:
        """Write aggregated data to MinIO."""
        output_path = f"s3a://taxi-processed/aggregates/{aggregate_type}/year={year}/month={month:02d}"

        logger.info(f"Writing {aggregate_type} aggregates to: {output_path}")

        df.write.mode("overwrite").parquet(output_path)

        return output_path

    def process(self, taxi_type: str, year: int, month: int) -> dict:
        """Run feature engineering pipeline."""
        logger.info(f"Creating features for {taxi_type} {year}-{month:02d}")

        # Read processed data
        df = self.read_processed_data(taxi_type, year, month)

        # Create aggregates
        hourly = self.create_hourly_aggregates(df)
        location = self.create_location_aggregates(df)
        daily = self.create_daily_summary(df)

        # Write aggregates
        self.write_aggregates(hourly, "hourly", year, month)
        self.write_aggregates(location, "location", year, month)
        self.write_aggregates(daily, "daily", year, month)

        return {
            "hourly_rows": hourly.count(),
            "location_rows": location.count(),
            "daily_rows": daily.count(),
        }


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Feature engineering")
    parser.add_argument("--taxi-type", default="yellow")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)

    args = parser.parse_args()

    spark = get_spark_session("TaxiFeatureEngineering")

    try:
        engineer = FeatureEngineer(spark)
        result = engineer.process(
            taxi_type=args.taxi_type, year=args.year, month=args.month
        )

        logger.info("Feature engineering complete!")
        logger.info(f"  Hourly aggregates: {result['hourly_rows']:,} rows")
        logger.info(f"  Location aggregates: {result['location_rows']:,} rows")
        logger.info(f"  Daily summary: {result['daily_rows']:,} rows")

    finally:
        stop_spark_session(spark)

    return 0


if __name__ == "__main__":
    sys.exit(main())
