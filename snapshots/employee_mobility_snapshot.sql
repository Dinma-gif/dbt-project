{% snapshot employee_mobility_snapshot %}
    {{
        config(
            unique_key="unique_key",
            strategy="check",
            check_cols=["role", "manager", "previous_job_level", "functional_group"],
            updated_at="date_of_mobility",
        )
    }}

    -- Selecting the data from the source table
    with
        final as (
            select
                trim(name) as name,
                cast(date_of_mobility as date) as date_of_mobility,  -- Date when the employee assumes the new role
                trim(previous_role) as role,  -- Employee's previous role
                case
                    when previous_manager = '#N/A' then null else previous_manager
                end as previous_manager,  -- Previous manager
                case
                    when previous_job_level = 'N/A'
                    then null  -- If job level is 'N/A', treat it as NULL
                    else previous_job_level
                end as previous_job_level,
                trim(previous_function) as previous_functional_group,  -- Functional group of the employee

                -- Create a concatenated unique key
                concat(trim(name), '_', cast(date_of_mobility as string)) as unique_key,  -- Concatenate name and date_of_mobility to create unique_key

                -- Logic for handling the start and end dates of the record
                cast(date_of_mobility as date) as valid_from,

                -- The date the employee assumed the new role (start of current version)
                -- LEAD function to get the next date_of_mobility for valid_to
                lead(date_of_mobility) over (
                    partition by name order by date_of_mobility
                ) as valid_to,  -- Lookahead for the next role change

            from {{ ref("stg_hr_staff_mobility") }}  -- Pulling data from the staging table
            where name is not null and name != '#N/A' 

        )

    -- Final selection, ensuring we only get the most recent record for each employee
    select *
    from final
    where
        valid_from is not null  -- Make sure there is a valid mobility date
        and (valid_from != valid_to or valid_to is null)  -- Only the most recent record will have NULL for valid_to
{% endsnapshot %}
