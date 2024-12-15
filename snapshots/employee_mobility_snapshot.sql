{% snapshot employee_mobility_snapshot %}
    {{
        config(
            target_schema="snapshots",
            unique_key="employee_name_and_date",
            strategy="timestamp",
            updated_at="date_of_mobility"
        )
    }}

    with
        hr_staff_mobility_rw as (
            select * from {{ source("public", "sheet2") }}
            -- Add any filters you need for the source here if required
            {% if is_incremental() %}
                -- Only select new records since the last snapshot
                AND date_of_mobility >= (SELECT MAX(date_of_mobility) FROM {{ this }})
            {% endif %}
        ),

        mobility_changes as (
            select
                concat(
                    trim(name), '-', date(date_of_mobility)
                ) as employee_name_and_date,
                trim(name) as name,
                date(date_of_mobility) as start_date,
                coalesce(previous_role, 'unknown') as previous_role,
                coalesce(previous_manager, 'unknown') as previous_manager,
                coalesce(previous_job_level, 'unknown') as previous_job_level,
                coalesce(previous_functional_group, 'unknown') as previous_functional_group
            from hr_staff_mobility_rw
        )

    select
        employee_name_and_date,
        name,
        start_date,
        previous_role,
        previous_manager,
        previous_job_level,
        previous_functional_group
    from mobility_changes

{% endsnapshot %}
