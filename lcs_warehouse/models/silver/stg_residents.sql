WITH source AS (
    SELECT * FROM {{ source('raw', 'residents') }}
),

cleaned AS (
    SELECT
        resident_id,
        facility_id,
        first_name,
        last_name,
        first_name || ' ' || last_name AS full_name,
        age,
        care_level,
        admission_date::DATE AS admission_date,
        discharge_date::DATE AS discharge_date,
        DATEDIFF(day, admission_date, COALESCE(discharge_date, CURRENT_DATE)) AS length_of_stay_days
    FROM source
    WHERE resident_id IS NOT NULL
)

SELECT * FROM cleaned
