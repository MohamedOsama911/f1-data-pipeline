-- This model creates a clean, final table for driver standings.
SELECT
    standings.season,
    standings.position AS championship_rank,
    standings.points,
    standings.wins,
    drivers.driver_name,
    drivers.nationality
FROM {{ ref('stg_ergast__driver_standings') }} AS standings
LEFT JOIN {{ ref('dim_drivers') }} AS drivers
    ON standings.driver_id = drivers.driver_id
ORDER BY
    standings.season DESC, championship_rank