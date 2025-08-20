-- This model creates a unique list of drivers with their details.
WITH driver_details AS (
    SELECT
        "DRIVER_DRIVERID" AS driver_id,
        "DRIVER_GIVENNAME" || ' ' || "DRIVER_FAMILYNAME" AS driver_name,
        "DRIVER_NATIONALITY" AS nationality,
        "DRIVER_DATEOFBIRTH"::DATE AS date_of_birth 
    FROM {{ source('ergast_raw', 'RAW_RESULTS') }}
)
-- Use a window function to handle any potential duplicates and get one unique record per driver
SELECT
    driver_id,
    driver_name,
    nationality,
    date_of_birth,
    DATEDIFF(year, date_of_birth, CURRENT_DATE()) AS current_age
FROM driver_details
QUALIFY ROW_NUMBER() OVER (PARTITION BY driver_id ORDER BY date_of_birth DESC) = 1