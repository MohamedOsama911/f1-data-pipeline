SELECT
    "SEASON"::INT AS season,
    "POSITION"::INT AS position,
    "POINTS"::INT AS points,
    "WINS"::INT AS wins,
    "DRIVER_DRIVERID" AS driver_id
FROM
    {{ source('ergast_raw', 'RAW_DRIVER_STANDINGS') }}