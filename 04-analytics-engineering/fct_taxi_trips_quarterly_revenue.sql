{{ config(materialized='table') }}

with trips_data as (
    select *,  from {{ ref('fact_trips') }}
),
trips_data_add as(
	SELECT 
	service_type,
	EXTRACT(YEAR FROM pickup_datetime) AS year,
    EXTRACT(QUARTER FROM pickup_datetime) AS quarter,
	SUM(total_amount) AS revenue
    from trips_data
    WHERE EXTRACT(YEAR FROM pickup_datetime) IN (2019, 2020)
    GROUP BY service_type,year,quarter	
)
Select 
	year,
	quarter,
	service_type,
	revenue,
	LAG(revenue) OVER (PARTITION BY service_type, quarter ORDER BY year) AS prev_year_revenue,
    (revenue - LAG(revenue) OVER (PARTITION BY service_type, quarter ORDER BY year)) / 
        NULLIF(LAG(revenue) OVER (PARTITION BY service_type, quarter ORDER BY year), 0) AS yoy_growth
FROM trips_data_add

--dbt build --select +fct_taxi_trips_quarterly_revenue.sql+ --vars '{is_test_run: false}'