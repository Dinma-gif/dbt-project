{{ 
    config(
        materialized='table', 
        tags=['employee'] 
    )
}}

WITH hr_staff_current AS (
    -- Extract current staff data from the source
    SELECT 
        name AS current_staff_name,
        email AS current_staff_email,
        role AS current_staff_role,
        job_level AS current_staff_job_level,
        manager_name,
        start_date,
        nationality,
        residence,
        gender,
        business_group  
    FROM {{ ref('stg_hr_staff_current') }}
),
db_staff AS (
    -- Extract staff data from the database staff table
    SELECT 
        staff_id,
        name AS db_staff_name,
        email AS db_staff_email,
        position AS db_staff_position,
        position_level
    FROM {{ ref('stg_db_staff') }}
),
hr_staff_mobility AS (
    -- Extract role mobility data
    SELECT 
        name AS mobility_name,  
        date_of_mobility,
        previous_role,
        previous_manager
    FROM {{ ref('stg_hr_staff_mobility') }}  
),
staff_history AS (
    -- Join the three data sources into a single history table
    SELECT 
        hsc.*,  
        hsm.date_of_mobility,
        hsm.previous_role,
        hsm.previous_manager,
        ds.staff_id,
        ds.db_staff_name,
        ds.db_staff_email,
        ds.db_staff_position,
        ds.position_level,
        hsm.mobility_name
    FROM hr_staff_current hsc
    LEFT JOIN hr_staff_mobility hsm
        ON hsc.current_staff_name = hsm.mobility_name
    LEFT JOIN db_staff ds
        ON hsc.current_staff_email = ds.db_staff_email
),
staff_with_row_number AS (
    -- Calculate row number to ensure the most recent record is selected
    SELECT 
        COALESCE(sh.current_staff_name, sh.db_staff_name, sh.mobility_name) AS staff_name,
        COALESCE(sh.current_staff_email, sh.db_staff_email, 'unknown') AS staff_email,
        COALESCE(sh.current_staff_role, sh.db_staff_position,'unknown') AS staff_role,
        COALESCE(sh.current_staff_job_level, sh.position_level) AS current_staff_job_level,
        COALESCE(sh.manager_name, sh.previous_manager) AS manager_name,
        date_of_mobility,
        previous_role,
        previous_manager,
        residence,
        start_date,
        nationality,
        gender,
        business_group,
        ROW_NUMBER() OVER (
            PARTITION BY sh.previous_role 
            ORDER BY sh.date_of_mobility DESC
        ) AS row_num
    FROM staff_history sh
)

-- Final selection to get the most recent role change for each employee
SELECT * 
FROM staff_with_row_number
WHERE row_num = 1
ORDER BY date_of_mobility DESC
