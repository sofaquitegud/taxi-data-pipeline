import os
from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "TaxiDataPipeline") -> SparkSession:
    """Create and return a Spark session configured for the pipeline"""
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")

    # Hadoop AWS jars for S3A support
    hadoop_aws_jar = "/opt/spark/jars/hadoop-aws-3.3.4.jar"
    aws_sdk_jar = "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar"

    spark = (
        SparkSession.builder.appName(app_name)  # type: ignore[attr-defined]
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        # Add Hadoop AWS jars
        .config("spark.jars", f"{hadoop_aws_jar},{aws_sdk_jar}")
        # S3A/MinIO configuration
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{minio_endpoint}")
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # Performance settings
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

    # Set log level
    spark.sparkContext.setLogLevel("WARN")

    return spark


def stop_spark_session(spark: SparkSession):
    """Stop Spark session"""
    if spark:
        spark.stop()
