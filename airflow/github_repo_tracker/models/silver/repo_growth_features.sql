{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        on_schema_change='sync_all_columns'
    )
}}

with base as (
    select
        _batch_id,
        _ingested_at,
        repo_id,
        repo_name,
        full_name,
        html_url,
        owner_login,
        language,
        topics_json,
        stargazers_count,
        forks_count,
        watchers_count,
        open_issues_count,
        pushed_at
    from {{ ref('stg_repos') }}

    {% if_incremental() %}
        where _ingested_at > (select max(_ingested_at) from {{ this}})
    {% endif %}
),

lags as (
    select *,
        lag(stargazers_count) over(partition by repo_id order by _ingested_at) as prev_stargazers_count,
        lag(forks_count) over(partition by repo_id order by _ingested_at) as prev_forks_count,
        lag(watchers_count) over(partition by repo_id order by _ingested_at) as prev_watchers_count
    from base
),

features as (
    select
        _batch_id,
        _ingested_at,
        repo_id,
        repo_name,
        full_name,
        html_url,
        owner_login,
        language,
        topics_json,
        stargazers_count,
        forks_count,
        watchers_count,
        open_issues_count,
        pushed_at,

        coalesce(prev_stargazers_count, stargazers_count) as prev_stargazers_count,
        coalesce(prev_forks_count, forks_count) as prev_forks_count,
        coalesce(prev_watchers_count, watchers_count) as prev_watchers_count,

        greatest(stargazers_count - coalesce(prev_stargazers_count, stargazers_count), 0) as delta_stars,
        greatest(forks_count - coalesce(prev_forks_count, forks_count), 0) as delta_forks,
        greatest(watchers_count - coalesce(prev_watchers_count, watchers_count), 0) as delta_watchers,

        greatest(stargazers_count - coalesce(prev_stargazers_count, stargazers_count), 0) as delta_stars,
        greatest(forks_count - coalesce(prev_forks_count, forks_count), 0) as delta_forks,
        greatest(watchers_count - coalesce(prev_watchers_count, watchers_count), 0) as delta_watchers,

        case
            when pushed_at >= (_ingested_at - interval '1' day) then 1
            else 0
        end as activity_flag
    from lags
),

