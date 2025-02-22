WITH PRE AS (
    SELECT 
        *, 
        timestamp_diff(dropoff_datetime,pickup_datetime,second) as trip_duration, 
    FROM {{ref('fact_fhv_trips')}}
), PRE2 AS (
    SELECT DISTINCT 
        pickup_year,
        pickup_month,
        pickup_locationid,
        pickup_zone,
        dropoff_locationid,
        dropoff_zone, 
        PERCENTILE_CONT(trip_duration, 0.90) OVER (PARTITION BY pickup_year,
            pickup_month,
            pickup_locationid,
            pickup_zone,
            dropoff_locationid,
            dropoff_zone) AS duration_p90 
    from PRE 
) 
SELECT 
    *,
    RANK() OVER (PARTITION BY pickup_year, pickup_month, pickup_zone ORDER BY duration_p90 DESC) as rk 
FROM PRE2 

/* Q7 
SELECT * 
FROM `kestra-sandbox-448902.ny_taxi.fct_fhv_monthly_zone_traveltime_p90` 
where pickup_year = 2019 and pickup_month = 11 and pickup_zone in ('Newark Airport','SoHo','Yorkville East')
and rk = 2

LaGuardia Airport, Chinatown, Garment District
*/ 