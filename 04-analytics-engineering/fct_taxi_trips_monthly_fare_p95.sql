{{ config(materialized='table') }}

with trips_data as (
    select *,  from {{ ref('fact_trips') }}
),
trips_data_add as(
	SELECT 
		service_type,
		EXTRACT(YEAR FROM pickup_datetime) AS year,
		EXTRACT(MONTH FROM pickup_datetime) AS month,
		fare_amount
    from trips_data
    WHERE EXTRACT(YEAR FROM pickup_datetime) IN (2019, 2020) AND fare_amount > 0 AND trip_distance > 0 AND payment_type_description IN ('Cash', 'Credit card')
),
trips_data_percentile as(
SELECT 
    service_type,
    year,
    month,
    PERCENTILE_CONT(fare_amount, 0.97) OVER (PARTITION BY service_type, year, month) AS p97,
    PERCENTILE_CONT(fare_amount, 0.95) OVER (PARTITION BY service_type, year, month) AS p95,
    PERCENTILE_CONT(fare_amount, 0.90) OVER (PARTITION BY service_type, year, month) AS p90,
	row_number() over(partition by service_type, year, month) as rn
FROM trips_data_add
)
Select
	service_type,
    year,
    month,
	p97,
	p95,
	p90
from trips_data_percentile
where rn = 1

--dbt build --select +fct_taxi_trips_monthly_fare_p95.sql+ --vars '{is_test_run: false}'