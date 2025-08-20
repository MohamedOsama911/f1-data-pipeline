-- This model creates a unique list of constructors (teams).
SELECT DISTINCT
    "CONSTRUCTOR_CONSTRUCTORID" AS constructor_id,
    "CONSTRUCTOR_NAME" AS constructor_name,
    "CONSTRUCTOR_NATIONALITY" AS nationality
FROM {{ source('ergast_raw', 'RAW_RESULTS') }}