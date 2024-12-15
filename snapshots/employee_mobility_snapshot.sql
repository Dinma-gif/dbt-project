{% snapshot employee_mobility_snapshot %}
    {{
        config(
            unique_key='name',  
            strategy='check',   
            check_cols=['previous_role', 'previous_manager', 'previous_job_level', 'previous_functional_group'], 
            updated_at='date_of_mobility'  
        )
    }}

    -- Selecting the data from the source table
    SELECT
        trim(name) AS name,
        cast(date_of_mobility AS date) AS date_of_mobility,  -- date when the employee assumes the new role
        trim(previous_role) AS role,
        trim(previous_manager) AS manager,
        CASE 
            WHEN previous_job_level = 'N/A' THEN NULL
            ELSE previous_job_level
        END AS previous_job_level,
        trim(previous_functional_group) AS functional_group,
        
        -- Updated logic to handle valid_from and valid_to
        date_of_mobility AS valid_from,  -- The date the employee assumed the new role, this marks the start of the current version
        NULL AS valid_to,               -- valid_to is NULL for the current record
        1 AS current_flag               -- Mark the current version as active
        
    FROM {{ source("public", "sheet2") }}
    WHERE name IS NOT NULL AND name != '#N/A'

{% endsnapshot %}
