WITH
    db_staff_rw AS (
        SELECT * 
        FROM {{ source("public", "sheet3") }}
    ),

    db_staff_int AS (
        SELECT
            staff_id,
            trim(name) AS name,
            lower(trim(email)) AS email,
            trim(position) AS position,
            coalesce(cast(position_level AS varchar), 'unknown') AS position_level,
            -- Convert to valid JSON format
            replace(replace(replace(styles, '[', '["'), ']', '"]'), ',', '","') AS valid_json_array_styles,
            replace(replace(replace(industries, '[', '["'), ']', '"]'), ',', '","') AS valid_json_array_industries,
            replace(replace(replace(software, '[', '["'), ']', '"]'), ',', '","') AS valid_json_array_software,
            trim(citizenship) AS citizenship,
            trim(residence) AS residence,
            cast(created_at AS timestamp) AS created_at,
            cast(offboarded_at AS timestamp) AS offboarded_at
        FROM db_staff_rw
    ),

    styles_expanded AS (
        SELECT 
            staff_id, 
            style.value::string AS style
        FROM 
            db_staff_int,
            LATERAL FLATTEN(input => parse_json(valid_json_array_styles)) AS style
    ),

    industries_expanded AS (
        SELECT 
            staff_id, 
            industry.value::string AS industry
        FROM 
            db_staff_int,
            LATERAL FLATTEN(input => parse_json(valid_json_array_industries)) AS industry
    ),

    software_expanded AS (
        SELECT 
            staff_id, 
            software.value::string AS software
        FROM 
            db_staff_int,
            LATERAL FLATTEN(input => parse_json(valid_json_array_software)) AS software
    )

SELECT 
    si.staff_id,
    si.name,
    si.email,
    si.position,
    si.position_level,
    si.citizenship,
    si.residence,
    si.created_at,
    si.offboarded_at,
    s.style,
    i.industry,
    sw.software
FROM 
    styles_expanded s
JOIN 
    industries_expanded i ON s.staff_id = i.staff_id
JOIN 
    software_expanded sw ON s.staff_id = sw.staff_id
JOIN 
    db_staff_int si ON s.staff_id = si.staff_id
