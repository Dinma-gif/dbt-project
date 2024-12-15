{{
    config(
        materialized='incremental', 
        on_schema_change='append_new_columns',  
        tags=['employee'] 
    )
}}

with
    hr_staff_current_rw as (
        select *
        from {{ source("public", "sheet1") }}
        where true
        -- {% if is_incremental() %}
        --    AND start_date >= (SELECT MAX(start_date) FROM {{ this }})
        -- {% endif %}
    )

, hr_staff_current_int as (
    select 
        lower(trim(email)) AS email,
        trim(name) AS name,
        coalesce(trim(role), 'unknown') AS role,
        job_level,
        lower(trim(manager_email)) AS manager_email,
        cast(start_date AS date) AS start_date,
        REGEXP_REPLACE(trim(nationality), '\\(.*\\)', '') AS nationality, 
        --trim(nationality) AS nationality,
        trim(residence) AS residence,
        coalesce(gender, 'unknown') AS gender,
        trim(business_group) AS business_group
    from hr_staff_current_rw
)

, hr_staff_current_final AS (
    select 
        email,
        name,
        role,
        job_level,
        manager_email,
        start_date,
        trim(split_part(nationality, ',', 1)) AS nationality,
        residence,
        gender,
        business_group
    from hr_staff_current_int
    union all 
    select 
        email,
        name,
        role,
        job_level,
        manager_email,
        start_date,
        trim(split_part(nationality, ',', 2)) AS nationality,
        residence,
        gender,
        business_group
    from hr_staff_current_int
    where length(nationality) - length(replace(nationality, ',', '')) >= 1
)

, final as (
    select 
        distinct
        email,
        name,
        role,
        job_level,
        manager_email,
        start_date,
        nationality,
        residence,
        gender,
        business_group
    from hr_staff_current_final
)

select * 
from final
order by email, nationality
