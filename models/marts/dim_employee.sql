{{
    config(
        materialized='incremental', 
        on_schema_change='append_new_columns',  
        tags=['employee'] 
    )
}}

with hr_staff_current as (
    select * from 
    {{ref('stg_hr_staff_current')
    }}
)
, hr_staff_mobility as (
    select * from
    {{ref('stg_hr_staff_mobility')
    }}
)
, db_staff as (
    select * from 
    {{ref('stg_db_staff')}}
)
,
    final as (
        SELECT 
            coalesce(t3.staff_id, row_number() over ()) as staff_id,
            coalesce(t1.email, t2.email,t3.email) as email,
            coalesce(t1.name,t2.name,t3.name) as name,
            coalesce(t1.role,t3.position,'unknown') as role,
            coalesce(t1.job_level,t3.position_level) as job_level,
            t1.manager_email,
            t1.start_date,
            t1.nationality,
            t1.residence,
            t1.gender,
            t1.business_group,
            t3.created_at
        from hr_staff_current 
        join db_staff on hr_staff_current.email = db_staff.email )