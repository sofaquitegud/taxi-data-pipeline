import logging
import os
import sys
import tempfile
from datetime import datetime
from typing import Optional, Tuple, List
import requests
import pyarrow.parquet as pq
from config import tlc_config, minio_config
from minio_client import minio_client
from schema import get_required_columns

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class TLCDataDownloader:
    """Download and store NYC TLC taxi data."""

    def __init__(self):
        """Initialize the downloader."""
        self.base_url = tlc_config.base_url
        self.bucket_raw = minio_config.bucket_raw

    def _generate_url(self, taxi_type: str, year: int, month: int) -> str:
        """Generate download URL for specific month's data."""
        filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
        return f"{self.base_url}/{filename}"

    def _generate_object_name(self, taxi_type: str, year: int, month: int) -> str:
        """Generate MinIO object name with partitioning."""
        return f"{taxi_type}/year={year}/month={month:02d}/{taxi_type}_tripdata_{year}-{month:02d}.parquet"

    def _validate_parquet(self, file_path: str, taxi_type: str) -> Tuple[bool, str]:
        """Validate downloaded parquet file."""
        try:
            # Read parquet metadata
            parquet_file = pq.ParquetFile(file_path)
            schema = parquet_file.schema_arrow
            num_rows = parquet_file.metadata.num_rows

            # Check if file has data
            if num_rows == 0:
                return False, "File contains no rows"

            # Check for required columns
            column_names = [field.name for field in schema]
            required_cols = get_required_columns(taxi_type)

            missing_cols = [col for col in required_cols if col not in column_names]
            if missing_cols:
                return False, f"Missing required columns: {missing_cols}"

            logger.info(
                f"Validation passed: {num_rows:,} rows, {len(column_names)} columns"
            )
            return True, f"Valid: {num_rows:,} rows"

        except Exception as e:
            return False, f"Validation error: {str(e)}"

    def download_month(
        self, taxi_type: str, year: int, month: int, skip_existing: bool = True
    ) -> Tuple[bool, str]:
        """Download data for a specific month."""
        object_name = self._generate_object_name(taxi_type, year, month)

        # Check if already exists
        if skip_existing and minio_client.object_exists(self.bucket_raw, object_name):
            logger.info(f"Skipping {object_name} - already exists")
            return True, "Already exists"

        url = self._generate_url(taxi_type, year, month)
        logger.info(f"Downloading from {url}")

        try:
            # Download file
            response = requests.get(url, stream=True, timeout=300)
            response.raise_for_status()

            # Save to temp file
            with tempfile.NamedTemporaryFile(
                suffix=".parquet", delete=False
            ) as tmp_file:
                for chunk in response.iter_content(chunk_size=8192):
                    tmp_file.write(chunk)
                tmp_path = tmp_file.name

            # Validate file
            is_valid, validation_msg = self._validate_parquet(tmp_path, taxi_type)
            if not is_valid:
                os.unlink(tmp_path)
                return False, validation_msg

            # Upload to MinIO
            success = minio_client.upload_file(
                bucket_name=self.bucket_raw,
                object_name=object_name,
                file_path=tmp_path,
                content_type="application/octet-stream",
            )

            # Clean up temp file
            os.unlink(tmp_path)

            if success:
                return True, f"Downloaded and uploaded: {validation_msg}"
            else:
                return False, "Failed to upload to MinIO"

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                return False, f"Data not available for {year}-{month:02d}"
            return False, f"HTTP error: {str(e)}"
        except requests.exceptions.RequestException as e:
            return False, f"Download error: {str(e)}"
        except Exception as e:
            return False, f"Unexpected error: {str(e)}"

    def download_range(
        self,
        taxi_type: str,
        start_year: int,
        start_month: int,
        end_year: int,
        end_month: int,
        skip_existing: bool = True,
    ) -> List[dict]:
        """Download data for a range of months."""
        results = []

        current_year = start_year
        current_month = start_month

        while (current_year < end_year) or (
            current_year == end_year and current_month <= end_month
        ):

            success, message = self.download_month(
                taxi_type=taxi_type,
                year=current_year,
                month=current_month,
                skip_existing=skip_existing,
            )

            results.append(
                {
                    "taxi_type": taxi_type,
                    "year": current_year,
                    "month": current_month,
                    "success": success,
                    "message": message,
                }
            )

            # Move to next month
            current_month += 1
            if current_month > 12:
                current_month = 1
                current_year += 1

        return results


def main():
    """Main entry point for downloading TLC data."""
    import argparse

    parser = argparse.ArgumentParser(description="Download NYC TLC taxi data")
    parser.add_argument(
        "--taxi-type",
        choices=["yellow", "green", "fhv", "fhvhv"],
        default="yellow",
        help="Type of taxi data to download",
    )
    parser.add_argument("--start-year", type=int, default=2023)
    parser.add_argument("--start-month", type=int, default=1)
    parser.add_argument("--end-year", type=int, default=2023)
    parser.add_argument("--end-month", type=int, default=3)
    parser.add_argument(
        "--force", action="store_true", help="Re-download even if file exists"
    )

    args = parser.parse_args()

    downloader = TLCDataDownloader()

    logger.info(f"Starting download: {args.taxi_type} taxi data")
    logger.info(
        f"Range: {args.start_year}-{args.start_month:02d} to {args.end_year}-{args.end_month:02d}"
    )

    results = downloader.download_range(
        taxi_type=args.taxi_type,
        start_year=args.start_year,
        start_month=args.start_month,
        end_year=args.end_year,
        end_month=args.end_month,
        skip_existing=not args.force,
    )

    # Print summary
    successful = sum(1 for r in results if r["success"])
    failed = len(results) - successful

    logger.info(f"\n{'='*50}")
    logger.info(f"Download complete: {successful} successful, {failed} failed")

    for result in results:
        status = "✓" if result["success"] else "✗"
        logger.info(
            f"  {status} {result['year']}-{result['month']:02d}: {result['message']}"
        )

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
