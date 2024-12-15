{{ 
    config(
        materialized='table', 
        tags=['employee'] 
    )
}}

WITH
-- Step 1: Select the most recent current HR staff data
hr_staff_current AS (
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
        ROW_NUMBER() OVER (PARTITION BY email ORDER BY start_date DESC) AS row_num  -- Most recent entry
    FROM {{ ref('stg_hr_staff_current') }}
),
-- Step 2: Select the most recent db staff data
db_staff AS (
    SELECT 
        staff_id,
        email,
        name,
        position AS role,
        position_level AS job_level,
        created_at,
        ROW_NUMBER() OVER (PARTITION BY email ORDER BY created_at DESC) AS row_num  -- Most recent entry
    FROM {{ ref('stg_db_staff') }}
),
-- Step 3: Select all mobility data for employees (do not limit to most recent)
hr_staff_mobility AS (
    SELECT 
        name,
        previous_role,
        previous_manager,
        previous_job_level,
        previous_functional_group,
        date_of_mobility,
        ROW_NUMBER() OVER (PARTITION BY name ORDER BY date_of_mobility DESC) AS row_num  -- Include all mobility events
    FROM {{ ref('employee_mobility_snapshot') }}
),
-- Step 4: Combine the most recent HR staff data with db staff data
final AS (
    SELECT 
        COALESCE(db_staff.staff_id, 'unknown') AS staff_id,
        COALESCE(hr_staff_current.email, db_staff.email, 'unknown') AS email,
        COALESCE(hr_staff_current.name, db_staff.name) AS name,
        COALESCE(hr_staff_current.role, db_staff.role, 'unknown') AS role,
        COALESCE(hr_staff_current.job_level, db_staff.job_level) AS job_level,
        hr_staff_current.manager_name,
        hr_staff_current.start_date,
        hr_staff_current.nationality,
        hr_staff_current.residence,
        hr_staff_current.gender,
        hr_staff_current.business_group,
        db_staff.created_at
    FROM hr_staff_current
    JOIN db_staff ON hr_staff_current.email = db_staff.email
    WHERE hr_staff_current.row_num = 1  -- Only the most recent HR staff entry
    AND db_staff.row_num = 1           -- Only the most recent DB staff entry
),
-- Step 5: Add all mobility data, calculate valid_from and valid_to for each mobility event
staff_history AS (
    SELECT 
        final.*,
        hr_staff_mobility.date_of_mobility AS valid_from,
        hr_staff_mobility.previous_role AS previous_role,
        hr_staff_mobility.previous_manager AS previous_manager,
        hr_staff_mobility.previous_job_level AS previous_job_level,
        hr_staff_mobility.previous_functional_group AS previous_functional_group,
        -- Get the date of the next mobility event for valid_to
        LEAD(hr_staff_mobility.date_of_mobility) OVER (PARTITION BY final.name ORDER BY hr_staff_mobility.date_of_mobility) AS next_mobility_date
    FROM final
    LEFT JOIN hr_staff_mobility ON final.name = hr_staff_mobility.name
)

-- Step 6: Final output with all mobility dates and roles, keeping the last active role
SELECT 
    staff_id,
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
    created_at,
    valid_from,
    CASE 
    WHEN next_mobility_date IS NOT NULL THEN next_mobility_date
        ELSE NULL  -- Set valid_to to NULL if no next mobility event (last role)
    END AS valid_to,
    previous_functional_group,
    previous_job_level,
    previous_manager,
    previous_role
    -- If no next mobility event, set valid_to to NULL (no end date for the last role)

FROM staff_history
WHERE valid_from IS NOT NULL  -- Make sure there is a valid mobility date
AND (valid_from != valid_to OR valid_to IS NULL)   
ORDER BY valid_from -- Order by the mobility date to see the progression
