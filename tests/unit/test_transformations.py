"""Unit tests for Spark transformation jobs."""

import pytest
from unittest.mock import Mock, patch, MagicMock
import sys
import os


class TestTaxiDataCleaner:
    """Test suite for taxi data cleaning."""

    def test_outlier_thresholds(self):
        """Test outlier threshold constants."""
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../spark/jobs"))
        from clean_taxi_data import TaxiDataCleaner

        assert TaxiDataCleaner.MAX_TRIP_DISTANCE == 500
        assert TaxiDataCleaner.MAX_FARE_AMOUNT == 1000
        assert TaxiDataCleaner.MAX_TOTAL_AMOUNT == 2000
        assert TaxiDataCleaner.MIN_PASSENGER_COUNT == 0
        assert TaxiDataCleaner.MAX_PASSENGER_COUNT == 10
        assert TaxiDataCleaner.MAX_TRIP_DURATION_HOURS == 24


class TestDataValidator:
    """Test suite for data validation utilities."""

    def test_validator_initialization(self):
        """Test validator initializes with DataFrame."""
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../spark/utils"))
        from data_validators import DataValidator

        mock_df = Mock()
        validator = DataValidator(mock_df)

        assert validator.df == mock_df
        assert validator.validation_results == []

    def test_check_row_count_passes(self):
        """Test row count check passes with sufficient rows."""
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../spark/utils"))
        from data_validators import DataValidator

        mock_df = Mock()
        mock_df.count.return_value = 10000

        validator = DataValidator(mock_df)
        passed, count = validator.check_row_count(min_rows=1000)

        assert passed == True
        assert count == 10000

    def test_check_row_count_fails(self):
        """Test row count check fails with insufficient rows."""
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../spark/utils"))
        from data_validators import DataValidator

        mock_df = Mock()
        mock_df.count.return_value = 100

        validator = DataValidator(mock_df)
        passed, count = validator.check_row_count(min_rows=1000)

        assert passed == False
        assert count == 100
