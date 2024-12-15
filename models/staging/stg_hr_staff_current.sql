{{ 
    config(
        materialized='incremental', 
        on_schema_change='append_new_columns',  
        tags=['employee'] 
    )
}}

WITH

-- Step 1: Select the raw data
hr_staff_current_rw AS (
    SELECT *
    FROM {{ source("public", "sheet1") }}
    WHERE true
    -- {% if is_incremental() %}
    --    AND start_date >= (SELECT MAX(start_date) FROM {{ this }})
    -- {% endif %}
),

-- Step 2: Clean and format the data
hr_staff_current_int AS (
    SELECT 
        LOWER(TRIM(email)) AS email,  -- Ensure emails are lowercase and trimmed
        TRIM(name) AS name,
        COALESCE(TRIM(role), 'unknown') AS role,
        job_level,
        LOWER(TRIM(manager_email)) AS manager_email,
        CAST(start_date AS date) AS start_date,
        REGEXP_REPLACE(TRIM(nationality), '\\(.*\\)', '') AS nationality,  -- Clean up nationality
        TRIM(residence) AS residence,
        COALESCE(gender, 'unknown') AS gender,
        CAST(birthday AS date) AS birthday,
        TRIM(business_group) AS business_group
    FROM hr_staff_current_rw
),

-- Step 3: Handle multiple nationalities by splitting into rows
hr_staff_current_final AS (
    SELECT 
        email,
        name,
        role,
        job_level,
        manager_email AS manager_name,
        start_date,
        TRIM(SPLIT_PART(nationality, ',', 1)) AS nationality,
        residence,
        gender,
        business_group
    FROM hr_staff_current_int
    UNION ALL 
    SELECT 
        email,
        name,
        role,
        job_level,
        manager_email AS manager_name,
        start_date,
        TRIM(SPLIT_PART(nationality, ',', 2)) AS nationality,
        residence,
        gender,
        business_group
    FROM hr_staff_current_int
    WHERE LENGTH(nationality) - LENGTH(REPLACE(nationality, ',', '')) >= 1  -- Handle multiple nationalities
),

-- Step 4: Add Row Number to get distinct records
final AS (
    SELECT 
        email,
        name,
        role,
        job_level,
        manager_name,
        start_date,
        nationality,
        residence,
        gender,
        business_group,
        ROW_NUMBER() OVER (PARTITION BY email ORDER BY start_date DESC) AS row_num  -- Ensure distinct records by email and order by start_date
    FROM hr_staff_current_final
)

-- Step 5: Final Selection (Return only the most recent record for each email)
SELECT 
    email,
    name,
    role,
    job_level,
    manager_name,
    start_date,
    nationality,
    residence,
    gender,
    business_group
FROM final
WHERE row_num = 1  -- Only take the most recent record for each email
ORDER BY email, nationality
