-- This model creates a clean, final table for constructor standings.
SELECT
    standings.season,
    standings.position AS championship_rank,
    standings.points,
    standings.wins,
    constructors.constructor_name,
    constructors.nationality
FROM {{ ref('stg_ergast__constructor_standings') }} AS standings
LEFT JOIN {{ ref('dim_constructors') }} AS constructors
    ON standings.constructor_id = constructors.constructor_id
ORDER BY
    standings.season DESC, championship_rank