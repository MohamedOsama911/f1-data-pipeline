-- This model creates a unique list of all races.
SELECT DISTINCT
    -- Create a unique ID for each race using season and round
    {{ dbt_utils.generate_surrogate_key(['season', 'round']) }} AS race_id,
    season,
    round,
    race_name,
    race_date,
    circuit_id,
    circuit_name
FROM
    {{ ref('stg_ergast__results') }} -- Use the clean staging table
ORDER BY
    season, round