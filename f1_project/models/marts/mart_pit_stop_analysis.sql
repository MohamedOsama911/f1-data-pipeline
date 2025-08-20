-- This model aggregates all pit stop data to get key metrics per team per season.
SELECT
    pit_stops.season,
    pit_stops.constructor_id,
    constructors.constructor_name,
    
    AVG(pit_stops.duration_seconds) AS average_pit_stop_seconds,
    MIN(pit_stops.duration_seconds) AS fastest_pit_stop_seconds,
    COUNT(1) AS total_pit_stops

FROM {{ ref('stg_ergast__pit_stops') }} AS pit_stops
LEFT JOIN {{ ref('dim_constructors') }} AS constructors
    ON TRY_CAST(NULLIF(pit_stops.constructor_id, '') AS INTEGER) = TRY_CAST(NULLIF(constructors.constructor_id, '') AS INTEGER)
WHERE pit_stops.duration_seconds IS NOT NULL  -- Filter out invalid duration values
GROUP BY
    pit_stops.season,
    pit_stops.constructor_id,
    constructors.constructor_name
ORDER BY
    average_pit_stop_seconds