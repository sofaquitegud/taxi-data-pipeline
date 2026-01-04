# Import libraries
import os
from dataclasses import dataclass


@dataclass
class MinioConfig:
    """MinIO connection configuration"""

    endpoint: str = os.getenv("MINIO_ENDPOINT", "minio:9000")
    access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    secure: bool = False
    bucket_raw: str = os.getenv("MINIO_BUCKET_RAW", "taxi-raw")
    bucket_processed: str = os.getenv("MINIO_BUCKET_PROCESSED", "taxi-processed")


@dataclass
class TLCDataConfig:
    """NYC TLC data source configuration"""

    base_url: str = "https://d37ci6vzurychx.cloudfront.net/trip-data"

    YELLOW_TAXI: str = "yellow"
    GREEN_TAXI: str = "green"
    FHV: str = "fhv"
    FHVHV: str = "fhvhv"

    default_start_year: int = 2023
    default_start_month: int = 1
    default_end_year: int = 2023
    default_end_month: int = 3


# Singleton instances
minio_config = MinioConfig()
tlc_config = TLCDataConfig()
