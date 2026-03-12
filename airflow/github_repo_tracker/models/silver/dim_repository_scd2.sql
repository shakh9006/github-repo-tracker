{{
    config(
        materialized='table'
    )
}}

select
    concat(cast(repo_id as varchar), '|', cast(dbt_valid_from as varchar)) as repo_scd_id,
    repo_id,
    full_name,
    description,
    owner,
    language,
    is_archived,
    is_fork,
    cast(dbt_valid_from as timestamp) as valid_from,
    cast(dbt_valid_to as timestamp) as valid_to,
    case when dbt_valid_to is null then true else false end as is_current
from {{ ref('snap_repo_scd2') }}