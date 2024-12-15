{{
    config(
        materialized='incremental', 
        on_schema_change='append_new_columns',  
        tags=['employee'] 
    )
}}

WITH
    hr_staff_mobility_rw AS (
        SELECT *
        FROM {{ source("public", "sheet2") }}
        -- {% if is_incremental() %}
        -- WHERE date_of_mobility >= (SELECT MAX(date_of_mobility) FROM {{ this }})
        -- {% endif %}
    ),

    final AS (
        SELECT
            trim(name) AS name,
            cast(date_of_mobility AS date) AS date_of_mobility,
            trim(previous_role) AS previous_role,
            trim(previous_manager) AS previous_manager,
            CASE 
                WHEN previous_job_level = 'N/A' THEN NULL
                ELSE previous_job_level
            END AS previous_job_level,
            trim(previous_functional_group) AS previous_function,
                            -- LEAD function to get the next date_of_mobility for valid_to

            ROW_NUMBER() OVER (PARTITION BY previous_role ORDER BY cast(date_of_mobility AS date) DESC) AS row_num  
        FROM hr_staff_mobility_rw
        WHERE name IS NOT NULL AND name != '#N/A' 
    )

-- Select the most recent row for each employee (row_num = 1)
SELECT
    name,
    date_of_mobility,
    previous_role,
    previous_manager,
    previous_job_level,
    previous_function
    -- If valid_to is null, leave it as NULL or use COALESCE() for a default value

FROM final
WHERE row_num = 1
 
