from dataclasses import dataclass
from typing import Dict, List, Any


@dataclass
class ColumnSchema:
    """Schema definition for a single column."""

    name: str
    dtype: str
    nullable: bool = True
    description: str = ""


# Yellow Taxi schema
YELLOW_TAXI_SCHEMA: List[ColumnSchema] = [
    ColumnSchema("VendorID", "int64", True, "TPEP provider code"),
    ColumnSchema("tpep_pickup_datetime", "datetime64[ns]", False, "Pickup datetime"),
    ColumnSchema("tpep_dropoff_datetime", "datetime64[ns]", False, "Dropoff datetime"),
    ColumnSchema("passenger_count", "float64", True, "Number of passengers"),
    ColumnSchema("trip_distance", "float64", True, "Trip distance in miles"),
    ColumnSchema("RatecodeID", "float64", True, "Rate code"),
    ColumnSchema("store_and_fwd_flag", "object", True, "Store and forward flag"),
    ColumnSchema("PULocationID", "int64", True, "Pickup location ID"),
    ColumnSchema("DOLocationID", "int64", True, "Dropoff location ID"),
    ColumnSchema("payment_type", "int64", True, "Payment type code"),
    ColumnSchema("fare_amount", "float64", True, "Fare amount"),
    ColumnSchema("extra", "float64", True, "Extra charges"),
    ColumnSchema("mta_tax", "float64", True, "MTA tax"),
    ColumnSchema("tip_amount", "float64", True, "Tip amount"),
    ColumnSchema("tolls_amount", "float64", True, "Tolls amount"),
    ColumnSchema("improvement_surcharge", "float64", True, "Improvement surcharge"),
    ColumnSchema("total_amount", "float64", True, "Total amount"),
    ColumnSchema("congestion_surcharge", "float64", True, "Congestion surcharge"),
    ColumnSchema("Airport_fee", "float64", True, "Airport fee"),
]

# Green Taxi schema
GREEN_TAXI_SCHEMA: List[ColumnSchema] = [
    ColumnSchema("VendorID", "int64", True, "LPEP provider code"),
    ColumnSchema("lpep_pickup_datetime", "datetime64[ns]", False, "Pickup datetime"),
    ColumnSchema("lpep_dropoff_datetime", "datetime64[ns]", False, "Dropoff datetime"),
    ColumnSchema("store_and_fwd_flag", "object", True, "Store and forward flag"),
    ColumnSchema("RatecodeID", "float64", True, "Rate code"),
    ColumnSchema("PULocationID", "int64", True, "Pickup location ID"),
    ColumnSchema("DOLocationID", "int64", True, "Dropoff location ID"),
    ColumnSchema("passenger_count", "float64", True, "Number of passengers"),
    ColumnSchema("trip_distance", "float64", True, "Trip distance in miles"),
    ColumnSchema("fare_amount", "float64", True, "Fare amount"),
    ColumnSchema("extra", "float64", True, "Extra charges"),
    ColumnSchema("mta_tax", "float64", True, "MTA tax"),
    ColumnSchema("tip_amount", "float64", True, "Tip amount"),
    ColumnSchema("tolls_amount", "float64", True, "Tolls amount"),
    ColumnSchema("ehail_fee", "float64", True, "E-hail fee"),
    ColumnSchema("improvement_surcharge", "float64", True, "Improvement surcharge"),
    ColumnSchema("total_amount", "float64", True, "Total amount"),
    ColumnSchema("payment_type", "float64", True, "Payment type code"),
    ColumnSchema("trip_type", "float64", True, "Trip type"),
    ColumnSchema("congestion_surcharge", "float64", True, "Congestion surcharge"),
]


def get_schema(taxi_type: str) -> List[ColumnSchema]:
    """Get schema for the given taxi type."""
    schemas = {
        "yellow": YELLOW_TAXI_SCHEMA,
        "green": GREEN_TAXI_SCHEMA,
    }
    return schemas.get(taxi_type, YELLOW_TAXI_SCHEMA)


def get_required_columns(taxi_type: str) -> List[str]:
    """Get list of required (non-nullable) columns."""
    schema = get_schema(taxi_type)
    return [col.name for col in schema if not col.nullable]


def get_column_names(taxi_type: str) -> List[str]:
    """Get all column names for a taxi type."""
    schema = get_schema(taxi_type)
    return [col.name for col in schema]
