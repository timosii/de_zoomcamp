{{ config(materialized='table') }}

WITH quarterly_revenue AS (
    SELECT
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(QUARTER FROM pickup_datetime) AS quarter,
        service_type,
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
        -- Get previous year's revenue
        LAG(curr.quarterly_revenue) OVER (PARTITION BY curr.service_type, curr.quarter ORDER BY curr.year) AS prev_year_revenue,
        -- Calculate YoY growth, handling division by zero
        CASE 
            WHEN LAG(curr.quarterly_revenue) OVER (PARTITION BY curr.service_type, curr.quarter ORDER BY curr.year) IS NULL 
                 OR LAG(curr.quarterly_revenue) OVER (PARTITION BY curr.service_type, curr.quarter ORDER BY curr.year) = 0 
            THEN NULL
            ELSE 
                ROUND(
                    ((curr.quarterly_revenue - LAG(curr.quarterly_revenue) OVER (PARTITION BY curr.service_type, curr.quarter ORDER BY curr.year)) 
                    / NULLIF(LAG(curr.quarterly_revenue) OVER (PARTITION BY curr.service_type, curr.quarter ORDER BY curr.year), 0)) * 100, 
                    2
                )
        END AS yoy_growth_percent
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
    year, quarter, service_type
