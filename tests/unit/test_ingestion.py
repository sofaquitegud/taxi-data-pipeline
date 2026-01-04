"""Unit tests for taxi data ingestion."""

import pytest
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add ingestion module to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../ingestion"))


class TestTLCDataDownloader:
    """Test suite for TLC data downloader."""

    def test_generate_url(self):
        """Test URL generation for taxi data."""
        from download_tlc_data import TLCDataDownloader

        downloader = TLCDataDownloader()
        url = downloader._generate_url("yellow", 2023, 1)

        assert "yellow_tripdata_2023-01.parquet" in url
        assert "d37ci6vzurychx.cloudfront.net" in url

    def test_generate_object_name(self):
        """Test MinIO object name generation."""
        from download_tlc_data import TLCDataDownloader

        downloader = TLCDataDownloader()
        object_name = downloader._generate_object_name("yellow", 2023, 1)

        assert (
            object_name == "yellow/year=2023/month=01/yellow_tripdata_2023-01.parquet"
        )

    def test_generate_object_name_green(self):
        """Test object name for green taxi."""
        from download_tlc_data import TLCDataDownloader

        downloader = TLCDataDownloader()
        object_name = downloader._generate_object_name("green", 2023, 12)

        assert object_name == "green/year=2023/month=12/green_tripdata_2023-12.parquet"


class TestSchemaValidation:
    """Test suite for schema validation."""

    def test_get_yellow_schema(self):
        """Test yellow taxi schema retrieval."""
        from schema import get_schema, YELLOW_TAXI_SCHEMA

        schema = get_schema("yellow")
        assert schema == YELLOW_TAXI_SCHEMA
        assert len(schema) > 0

    def test_get_green_schema(self):
        """Test green taxi schema retrieval."""
        from schema import get_schema, GREEN_TAXI_SCHEMA

        schema = get_schema("green")
        assert schema == GREEN_TAXI_SCHEMA

    def test_get_required_columns(self):
        """Test required columns extraction."""
        from schema import get_required_columns

        required = get_required_columns("yellow")
        assert "tpep_pickup_datetime" in required
        assert "tpep_dropoff_datetime" in required

    def test_get_column_names(self):
        """Test column names extraction."""
        from schema import get_column_names

        columns = get_column_names("yellow")
        assert "VendorID" in columns
        assert "passenger_count" in columns
        assert "total_amount" in columns


class TestMinioClient:
    """Test suite for MinIO client."""

    @patch("minio_client.Minio")
    def test_client_initialization(self, mock_minio):
        """Test MinIO client initializes correctly."""
        from minio_client import MinioClient

        client = MinioClient()
        assert client.bucket_raw == "taxi-raw"
        assert client.bucket_processed == "taxi-processed"

    @patch("minio_client.Minio")
    def test_object_exists_true(self, mock_minio):
        """Test object exists returns True when object exists."""
        from minio_client import MinioClient

        mock_instance = Mock()
        mock_instance.stat_object.return_value = Mock()
        mock_minio.return_value = mock_instance

        client = MinioClient()
        result = client.object_exists("test-bucket", "test-object")

        assert result == True
