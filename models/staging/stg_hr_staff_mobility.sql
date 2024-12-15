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
    --    WHERE true
    --     {% if is_incremental() %}
    --        AND date_of_mobility >= (SELECT MAX(date_of_mobility) FROM {{ this }})
    --     {% endif %}
    )

    , final as (
        select
            trim(name) as name,
            cast(date_of_mobility as date) as date_of_mobility,
            trim(previous_role) as previous_role,
            trim(previous_manager) as previous_manager,
            case 
                when previous_job_level = 'N/A' then NULL
                else previous_job_level
            end as previous_job_level,
            trim(previous_functional_group) as previous_function

        from hr_staff_mobility_rw
        where name is not null and name !='#N/A'
    )

    select * from final