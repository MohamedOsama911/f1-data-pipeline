-- This is the main fact table, bringing together data from multiple staging models.
WITH results AS (
    SELECT * FROM {{ ref('stg_ergast__results') }}
),
qualifying AS (
    SELECT * FROM {{ ref('stg_ergast__qualifying') }}
),
races AS (
    SELECT * FROM {{ ref('dim_races') }}
)
SELECT
    -- Primary Key for the fact table
    {{ dbt_utils.generate_surrogate_key(['results.season', 'results.round', 'results.driver_id']) }} AS race_result_id,
    
    -- Foreign Keys to Dimension tables
    races.race_id,
    results.driver_id,
    results.constructor_id,
    
    -- Performance Metrics
    qualifying.position AS qualifying_position,
    results.position AS race_finish_position,
    results.points AS points_scored,
    
    -- Boolean flag for easy filtering in Power BI
    results.fastest_lap_rank = 1 AS is_race_fastest_lap

FROM results
-- Join with qualifying to get the starting grid position
LEFT JOIN qualifying
    ON results.season = qualifying.season
    AND results.round = qualifying.round
    AND results.driver_id = qualifying.driver_id
-- Join with races to get the surrogate key for the race dimension
LEFT JOIN races
    ON results.season = races.season
    AND results.round = races.round