{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        on_schema_change='sync_all_columns',
        views_enabled=false
    )
}}

with src as (
    select
        _source_file,
        _batch_id,
        _ingested_at,
        raw_payload
    from {{ source('raw', 'github_repositories_raw') }}

    {% if is_incremental() %}
        where _ingested_at > (select max(_ingested_at) from {{ this}})
    {% endif %}
),

parsed as (
    select
        _source_file,
        _batch_id,
        _ingested_at,
        
        cast(json_extract_scalar(raw_payload, '$.id') as bigint) as repo_id,
        json_extract_scalar(raw_payload, '$.name') as repo_name,
        json_extract_scalar(raw_payload, '$.full_name') as full_name,
        json_extract_scalar(raw_payload, '$.description') as description,

        json_extract_scalar(raw_payload, '$.html_url') as html_url,
        json_extract_scalar(raw_payload, '$.language') as language,
        json_extract_scalar(raw_payload, '$.owner.login') as owner,

        cast(json_extract_scalar(raw_payload, '$.stargazers_count') as bigint) as stargazers_count,
        cast(json_extract_scalar(raw_payload, '$.forks_count') as bigint) as forks_count,
        cast(json_extract_scalar(raw_payload, '$.watchers_count') as bigint) as watchers_count,
        cast(json_extract_scalar(raw_payload, '$.open_issues_count') as bigint) as open_issues_count,

        from_iso8601_timestamp(json_extract_scalar(raw_payload, '$.created_at')) as created_at,
        from_iso8601_timestamp(json_extract_scalar(raw_payload, '$.updated_at')) as updated_at,
        from_iso8601_timestamp(json_extract_scalar(raw_payload, '$.pushed_at')) as pushed_at,

        cast(json_extract_scalar(raw_payload, '$.archived') as boolean) as is_archived,
        cast(json_extract_scalar(raw_payload, '$.fork') as boolean) as is_fork

    from src
    where json_extract_scalar(raw_payload, '$.id') is not null
),

dedup as (
    select *
    from (
        select
            *,
            row_number() over(
                partition by _batch_id, repo_id
                order by _ingested_at desc, _source_file desc
            ) as rn
        from parsed
    ) t
    where rn = 1
)

select
    _source_file,
    _batch_id,
    _ingested_at,
    repo_id,
    repo_name,
    full_name,
    description,
    html_url,
    owner,
    language,
    stargazers_count,
    forks_count,
    watchers_count,
    open_issues_count,
    created_at,
    updated_at,
    pushed_at,
    is_archived,
    is_fork
from dedup
