# Import libraries
import logging
from typing import List, Dict, Any, Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataValidator:
    """Validate Spark DataFrames for data quality."""

    def __init__(self, df: DataFrame):
        """Initialize with a DataFrame."""
        self.df = df
        self.validation_results = []

    def check_null_percentage(
        self, columns: List[str], max_null_pct: float = 0.1
    ) -> Tuple[bool, Dict[str, float]]:
        """Check null percentage in specified columns."""
        total_rows = self.df.count()
        null_percentages = {}
        all_passed = True

        for col in columns:
            null_count = self.df.filter(F.col(col).isNull()).count()
            null_pct = null_count / total_rows if total_rows > 0 else 0
            null_percentages[col] = null_pct

            if null_pct > max_null_pct:
                all_passed = False
                logger.warning(
                    f"Column {col} has {null_pct:.2%} nulls (max: {max_null_pct:.2%})"
                )

        return all_passed, null_percentages

    def check_value_range(
        self, column: str, min_val: float = None, max_val: float = None
    ) -> Tuple[bool, Dict[str, Any]]:
        """Check if values are within expected range."""
        stats = self.df.select(
            F.min(column).alias("min"),
            F.max(column).alias("max"),
            F.avg(column).alias("avg"),
        ).collect()[0]

        result = {"min": stats["min"], "max": stats["max"], "avg": stats["avg"]}

        passed = True
        if min_val is not None and stats["min"] is not None:
            if stats["min"] < min_val:
                passed = False
                logger.warning(f"Column {column} min value {stats['min']} < {min_val}")

        if max_val is not None and stats["max"] is not None:
            if stats["max"] > max_val:
                passed = False
                logger.warning(f"Column {column} max value {stats['max']} > {max_val}")

        return passed, result

    def check_unique_values(
        self, column: str, expected_values: List[Any] = None
    ) -> Tuple[bool, List[Any]]:
        """Check unique values in a column."""
        unique_values = [
            row[column] for row in self.df.select(column).distinct().collect()
        ]

        if expected_values:
            unexpected = set(unique_values) - set(expected_values)
            if unexpected:
                logger.warning(f"Unexpected values in {column}: {unexpected}")
                return False, unique_values

        return True, unique_values

    def check_row_count(
        self, min_rows: int = 1, max_rows: int = None
    ) -> Tuple[bool, int]:
        """Check row count is within bounds."""
        count = self.df.count()
        passed = True

        if count < min_rows:
            passed = False
            logger.warning(f"Row count {count} < minimum {min_rows}")

        if max_rows and count > max_rows:
            passed = False
            logger.warning(f"Row count {count} > maximum {max_rows}")

        return passed, count

    def run_all_checks(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Run all configured validation checks."""
        results = {"passed": True, "checks": {}}

        # Row count check
        if "row_count" in config:
            passed, count = self.check_row_count(**config["row_count"])
            results["checks"]["row_count"] = {"passed": passed, "count": count}
            if not passed:
                results["passed"] = False

        # Null percentage check
        if "null_columns" in config:
            passed, null_pcts = self.check_null_percentage(
                config["null_columns"]["columns"],
                config["null_columns"].get("max_pct", 0.1),
            )
            results["checks"]["null_percentage"] = {
                "passed": passed,
                "percentages": null_pcts,
            }
            if not passed:
                results["passed"] = False

        # Value range checks
        if "value_ranges" in config:
            for col, ranges in config["value_ranges"].items():
                passed, stats = self.check_value_range(col, **ranges)
                results["checks"][f"range_{col}"] = {"passed": passed, "stats": stats}
                if not passed:
                    results["passed"] = False

        return results
