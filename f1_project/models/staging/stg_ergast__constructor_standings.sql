SELECT
    "SEASON"::INT AS season,
    "POSITION"::INT AS position,
    "POINTS"::INT AS points,
    "WINS"::INT AS wins,
    "CONSTRUCTOR_CONSTRUCTORID" AS constructor_id,
    "CONSTRUCTOR_NAME" AS constructor_name
FROM
    {{ source('ergast_raw', 'RAW_CONSTRUCTOR_STANDINGS') }}