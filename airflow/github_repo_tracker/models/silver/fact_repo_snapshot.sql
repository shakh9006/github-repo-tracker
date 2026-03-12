{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        on_schema_change='sync_all_columns'
    )
}}

with src as (

    select
        _batch_id,
        _ingested_at as snapshot_at,
        repo_id,
        owner,
        stargazers_count,
        forks_count,
        watchers_count,
        open_issues_count,
        pushed_at
    from {{ ref('stg_repos') }}

    {% if is_incremental() %}
      where _ingested_at > (
          select coalesce(max(snapshot_at), timestamp '1970-01-01 00:00:00')
          from {{ this }}
      )
    {% endif %}

),

repo_scd_match as (

    select
        s.*,
        d.repo_scd_id
    from src s
    left join {{ ref('dim_repository_scd2') }} d
      on s.repo_id = d.repo_id
     and s.snapshot_at >= d.valid_from
     and s.snapshot_at < coalesce(d.valid_to, timestamp '9999-12-31 00:00:00')

)

select
    _batch_id,
    snapshot_at,
    repo_id,
    repo_scd_id,
    owner,
    stargazers_count,
    forks_count,
    watchers_count,
    open_issues_count,
    pushed_at
from repo_scd_match