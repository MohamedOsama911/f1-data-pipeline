SELECT
    "SEASON"::INT AS season,
    "ROUND"::INT AS round,
    "RACENAME" AS race_name,
    "DATE"::DATE AS race_date,
    "CIRCUIT_CIRCUITID" AS circuit_id,
    "CIRCUIT_CIRCUITNAME" AS circuit_name,
    "NUMBER"::INT AS driver_number,
    "POSITION"::INT AS position,
    "DRIVER_DRIVERID" AS driver_id,
    "CONSTRUCTOR_CONSTRUCTORID" AS constructor_id,
    "Q1" AS q1_time,
    "Q2" AS q2_time,
    "Q3" AS q3_time
FROM
    {{ source('ergast_raw', 'RAW_QUALIFYING') }}