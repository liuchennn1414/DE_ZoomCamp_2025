WITH Pre AS (
    SELECT 
        service_type
        , pickup_year
        , pickup_quarter
        , SUM(total_amount) AS quarterly_revenue
    FROM {{ref('fact_trips')}}
    WHERE pickup_year in (2019,2020)
    GROUP BY 
        service_type
        , pickup_year
        , pickup_quarter
), Final AS (
    SELECT 
        service_type
        , pickup_year
        , pickup_quarter
        , quarterly_revenue
        , LAG(quarterly_revenue) OVER (PARTITION BY service_type ORDER BY pickup_quarter,pickup_year) AS yoy_quarter_amount
        , SAFE_DIVIDE(quarterly_revenue - LAG(quarterly_revenue) OVER (PARTITION BY service_type ORDER BY pickup_quarter,pickup_year), 
        LAG(quarterly_revenue) OVER (PARTITION BY service_type ORDER BY pickup_quarter,pickup_year)) * 100  AS YOY_Q_GROWTH 
    FROM Pre
)
SELECT * FROM Final 

/* 
Question 5 
SELECT *  
FROM `kestra-sandbox-448902.ny_taxi.fct_taxi_trips_quarterly_revenue` 
ORDER BY service_type, pickup_quarter, pickup_year 
LIMIT 100

Answer: green: {best: 2020/Q1, worst: 2020/Q2}, yellow: {best: 2020/Q1, worst: 2020/Q2}
*/ 