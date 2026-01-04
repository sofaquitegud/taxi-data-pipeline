# Data Dictionary

## Source Data

NYC Taxi & Limousine Commission (TLC) Trip Record Data.

### Yellow Taxi Trips

| Column | Type | Description |
|--------|------|-------------|
| VendorID | Integer | TPEP provider (1=CMT, 2=VeriFone) |
| tpep_pickup_datetime | Timestamp | Pickup date and time |
| tpep_dropoff_datetime | Timestamp | Dropoff date and time |
| passenger_count | Integer | Number of passengers |
| trip_distance | Float | Trip distance in miles |
| RatecodeID | Integer | Rate code (1=Standard, 2=JFK, 3=Newark, etc.) |
| store_and_fwd_flag | String | Store and forward flag (Y/N) |
| PULocationID | Integer | Pickup location zone ID |
| DOLocationID | Integer | Dropoff location zone ID |
| payment_type | Integer | Payment method (1=Credit, 2=Cash, etc.) |
| fare_amount | Float | Base fare amount |
| extra | Float | Extra charges |
| mta_tax | Float | MTA tax |
| tip_amount | Float | Tip amount |
| tolls_amount | Float | Toll charges |
| improvement_surcharge | Float | Improvement surcharge |
| total_amount | Float | Total trip cost |
| congestion_surcharge | Float | Congestion surcharge |
| airport_fee | Float | Airport fee |

---

## Processed Data

### fact_trips

Central fact table for taxi trips.

| Column | Type | Description |
|--------|------|-------------|
| trip_id | String | Surrogate key |
| taxi_type | String | yellow or green |
| pickup_datetime | Timestamp | Pickup time |
| dropoff_datetime | Timestamp | Dropoff time |
| pickup_location_id | Integer | FK to dim_location |
| dropoff_location_id | Integer | FK to dim_location |
| payment_type_id | Integer | FK to dim_payment |
| rate_code_id | Integer | FK to dim_rate |
| passenger_count | Integer | Passengers |
| trip_distance_miles | Float | Distance |
| trip_duration_minutes | Float | Duration |
| avg_speed_mph | Float | Average speed |
| fare_amount | Float | Base fare |
| tip_amount | Float | Tip |
| total_amount | Float | Total cost |
| tip_percentage | Float | Tip as % of fare |

---

## Dimension Tables

### dim_datetime

| Column | Type | Description |
|--------|------|-------------|
| datetime_id | String | Primary key |
| full_datetime | Timestamp | Full timestamp |
| date_day | Date | Date only |
| year | Integer | Year |
| month | Integer | Month (1-12) |
| day | Integer | Day of month |
| hour | Integer | Hour (0-23) |
| day_of_week | Integer | Day of week (0-6) |
| is_weekend | Boolean | Weekend flag |
| time_of_day | String | morning/afternoon/evening/night |

### dim_location

| Column | Type | Description |
|--------|------|-------------|
| location_id | Integer | Primary key |
| zone_name | String | Zone name |
| borough | String | Borough name |
| service_zone | String | Service zone type |

### dim_payment

| Column | Type | Description |
|--------|------|-------------|
| payment_type_id | Integer | Primary key |
| payment_type | String | Payment name |
| description | String | Full description |

### dim_rate

| Column | Type | Description |
|--------|------|-------------|
| rate_code_id | Integer | Primary key |
| rate_code | String | Rate name |
| description | String | Full description |

---

## Aggregate Tables

### daily_trip_summary
Daily aggregated metrics by location and taxi type.

### hourly_demand_patterns
Hour-of-day and day-of-week demand patterns.

### zone_performance
Performance metrics by pickup/dropoff zone.
