# Import libraries
import logging
from io import BytesIO
from minio import Minio
from minio.error import S3Error
from config import minio_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MinioClient:
    """Wrapper class for MinIO operations"""

    def __init__(self):
        self.client = Minio(
            endpoint=minio_config.endpoint,
            access_key=minio_config.access_key,
            secret_key=minio_config.secret_key,
            secure=minio_config.secure,
        )
        self.bucket_raw = minio_config.bucket_raw
        self.bucket_processed = minio_config.bucket_processed

    def ensure_bucket_exists(self, bucket_name: str) -> bool:
        """Create bucket if it doesn't exist"""
        try:
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
                logger.info(f"Created bucket: {bucket_name}")
            return True
        except S3Error as e:
            logger.error(f"Error creating bucket {bucket_name}: {e}")
            return False

    def upload_file(
        self,
        bucket_name: str,
        object_name: str,
        file_path: str,
        content_type: str = "application/octet-stream",
    ) -> bool:
        """Upload a file to MinIO"""
        try:
            self.client.fput_object(
                bucket_name=bucket_name,
                object_name=object_name,
                file_path=file_path,
                content_type=content_type,
            )
            logger.info(f"Uploaded {object_name} to {bucket_name}")
            return True
        except S3Error as e:
            logger.error(f"Error uploading {object_name}: {e}")
            return False

    def upload_bytes(
        self,
        bucket_name: str,
        object_name: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> bool:
        """Upload bytes data to MinIO"""
        try:
            data_stream = BytesIO(data)
            self.client.put_object(
                bucket_name=bucket_name,
                object_name=object_name,
                data=data_stream,
                length=len(data),
                content_type=content_type,
            )
            logger.info(f"Uploaded {object_name} to {bucket_name}")
            return True
        except S3Error as e:
            logger.error(f"Error uploading {object_name}: {e}")
            return False

    def object_exists(self, bucket_name: str, object_name: str) -> bool:
        """Check if an object exists in the bucket"""
        try:
            self.client.stat_object(bucket_name, object_name)
            return True
        except S3Error:
            return False

    def list_objects(
        self, bucket_name: str, object_name: str, prefix: str = ""
    ) -> list:
        """List objects in a bucket with optional prefix"""
        try:
            objects = self.client.list_objects(
                bucket_name, prefix=prefix, recursive=True
            )
            return [obj.object_name for obj in objects]
        except S3Error as e:
            logger.error(f"Error listing objects: {e}")
            return []

    def download_file(self, bucket_name: str, object_name: str, file_path: str) -> bool:
        """Download a file from MinIO"""
        try:
            self.client.fget_object(
                bucket_name=bucket_name, object_name=object_name, file_path=file_path
            )
            logger.info(f"Downloaded {object_name} to {file_path}")
            return True
        except S3Error as e:
            logger.error(f"Error downloading {object_name} : {e}")
            return False


# Singleton instance
minio_client = MinioClient()
