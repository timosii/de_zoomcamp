{{ config(materialized='table') }}

WITH quarterly_revenue AS (
    SELECT
        -- Extract year and quarter from the pickup datetime
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(QUARTER FROM pickup_datetime) AS quarter,
        -- Assuming there is a 'taxi_type' column to differentiate between Green and Yellow Taxis
        service_type,
        -- Sum the total_amount for each quarter and taxi type
        SUM(total_amount) AS quarterly_revenue
    FROM
        {{ ref('fact_trips') }}
    GROUP BY
        year, quarter, service_type
),
yoy_growth AS (
    SELECT
        curr.year,
        curr.quarter,
        curr.service_type,
        curr.quarterly_revenue,
        -- Calculate YoY growth
        LAG(curr.quarterly_revenue) OVER (PARTITION BY curr.service_type, curr.quarter ORDER BY curr.year) AS prev_year_revenue,
        ROUND(
            ((curr.quarterly_revenue - LAG(curr.quarterly_revenue) OVER (PARTITION BY curr.service_type, curr.quarter ORDER BY curr.year)) 
            / LAG(curr.quarterly_revenue) OVER (PARTITION BY curr.service_type, curr.quarter ORDER BY curr.year)) * 100, 
            2
        ) AS yoy_growth_percent
    FROM
        quarterly_revenue curr
)
SELECT
    year,
    quarter,
    service_type,
    quarterly_revenue,
    prev_year_revenue,
    yoy_growth_percent
FROM
    yoy_growth
ORDER BY
    year, quarter, service_type;
