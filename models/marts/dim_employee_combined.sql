-- models/marts/dim_employee_combined.sql
{{ 
    config(
        materialized='table',
        tags=['employee', 'mobility_combined']
    )
}}

with
    hr_staff_mobility_rw AS (
        SELECT *
        FROM {{ source("public", "sheet2") }}

    ),
    -- Current employee mobility data (from incremental model)
    current_mobility as (
        select
            name,
            role as current_role,
            manager as current_manager,
            job_level as current_job_level,
            functional_group as current_functional_group,
            date_of_mobility as current_mobility_date,
            'current' as data_source  -- Label to distinguish from historical data
        from hr_staff_mobility_rw -- Referring to the incremental model

        {% if is_incremental() %}
            -- Only select new records or records where mobility data has changed
            where
                current_mobility_date
                > (select max(current_mobility_date) from {{ this }})
        {% endif %}
    ),

    -- Historical employee mobility data (from snapshot model)
    historical_mobility as (
        select
            name,
            previous_role as current_role,  -- Historical role
            previous_manager as current_manager,  -- Historical manager
            previous_job_level as current_job_level,  -- Historical job level
            previous_functional_group as current_functional_group,  -- Historical functional group
            start_date as current_mobility_date,  -- Historical date of mobility (snapshot)
            'historical' as data_source  -- Label to distinguish from current data
        from {{ ref("employee_mobility_snapshot") }}  -- Referring to the snapshot model
    )

-- Combine current and historical mobility data
select *
from current_mobility

union all

select *
from historical_mobility
