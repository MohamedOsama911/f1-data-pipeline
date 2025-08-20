SELECT
    "SEASON"::INT AS season,
    "ROUND"::INT AS round,
    "RACENAME" AS race_name,
    "DATE"::DATE AS race_date,
    "CIRCUIT_CIRCUITID" AS circuit_id,
    "CIRCUIT_CIRCUITNAME" AS circuit_name,
    "NUMBER"::INT AS driver_number,
    "POSITION"::INT AS position,
    "POINTS"::INT AS points,
    "GRID"::INT AS grid_position,
    "LAPS"::INT AS laps,
    "STATUS" AS status,
    "DRIVER_DRIVERID" AS driver_id,
    "CONSTRUCTOR_CONSTRUCTORID" AS constructor_id,
    "TIME_MILLIS"::INT AS race_time_millis,
    "FASTESTLAP_RANK"::INT AS fastest_lap_rank,
    "FASTESTLAP_LAP"::INT AS fastest_lap_number,
    "FASTESTLAP_TIME_TIME" AS fastest_lap_time
FROM
    {{ source('ergast_raw', 'RAW_RESULTS') }}