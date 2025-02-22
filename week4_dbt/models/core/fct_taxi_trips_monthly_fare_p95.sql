WITH filtered_trips AS (
    SELECT 
        service_type,
        pickup_year,
        pickup_month,
        fare_amount
    FROM {{ ref('fact_trips') }}
    WHERE 
        fare_amount > 0 
        AND trip_distance > 0 
        AND payment_type_description IN ('Cash', 'Credit card')
)
SELECT distinct 
    service_type,
    pickup_year,
    pickup_month,
    ROUND(PERCENTILE_CONT(fare_amount, 0.90) OVER (PARTITION BY service_type, pickup_year, pickup_month ),3) AS fare_p90,
    ROUND(PERCENTILE_CONT(fare_amount, 0.95) OVER (PARTITION BY service_type, pickup_year, pickup_month),3) AS fare_p95,
    ROUND(PERCENTILE_CONT(fare_amount, 0.97) OVER (PARTITION BY service_type, pickup_year, pickup_month),3) AS fare_p97
FROM filtered_trips

/* Q6 
SELECT * 
FROM tbl
WHERE pickup_year = 2020 and pickup_month = 4 

Answer: 
green: {p97: 55.0, p95: 45.0, p90: 26.5}, yellow: {p97: 32.0, p95: 26.0, p90: 19}
WHICH SHOULD BE 
green: {p97: 55.0, p95: 45.0, p90: 26.5}, yellow: {p97: 31.5, p95: 25.5, p90: 19.0}
*/ 