-- This model enriches the raw pit stop data.
-- The raw table only contains the event of the pit stop.
-- We join it with the results data to get the full context:
-- who the constructor was, what the circuit was, the name of the race, etc.

WITH race_context AS (
    -- Step 1: Create a rich context table from the results data for joining
    SELECT DISTINCT
        "SEASON"::INT AS season,
        "ROUND"::INT AS round,
        "RACENAME" AS race_name,
        "DATE"::DATE AS race_date,
        "CIRCUIT_CIRCUITID" AS circuit_id,
        "CIRCUIT_CIRCUITNAME" AS circuit_name,
        "DRIVER_DRIVERID" AS driver_id,
        "CONSTRUCTOR_CONSTRUCTORID" AS constructor_id
    FROM {{ source('ergast_raw', 'RAW_RESULTS') }}
)

SELECT
    -- Columns from our Pit Stops table (ps)
    ps."SEASON"::INT AS season,
    ps."ROUND"::INT AS round,
    ps."DRIVERID" AS driver_id,
    ps."STOP"::INT AS stop_number,
    ps."LAP"::INT AS lap,
    ps."TIME" AS time_of_day,
    TRY_CAST(NULLIF(ps."DURATION", '') AS FLOAT) AS duration_seconds,  -- Fixed this line

    -- Columns joined from our Race Context CTE (ctx)
    ctx.race_name,
    ctx.race_date,
    ctx.circuit_id,
    ctx.circuit_name,
    ctx.constructor_id
FROM
    {{ source('ergast_raw', 'RAW_PIT_STOPS') }} AS ps
LEFT JOIN race_context AS ctx
    -- The join key to link a pit stop to its race and driver/team context
    ON ps."SEASON"::INT = ctx.season
    AND ps."ROUND"::INT = ctx.round
    AND ps."DRIVERID" = ctx.driver_id