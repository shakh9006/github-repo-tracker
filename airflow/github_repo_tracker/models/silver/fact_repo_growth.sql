{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        on_schema_change='sync_all_columns'
    )
}}

with max_loaded as (

    {% if is_incremental() %}
    select coalesce(max(snapshot_at), timestamp '1970-01-01 00:00:00') as max_snapshot_at
    from {{ this }}
    {% else %}
    select timestamp '1970-01-01 00:00:00' as max_snapshot_at
    {% endif %}

),

new_rows as (

    select
        s.*,
        1 as is_new
    from {{ ref('fact_repo_snapshot') }} s
    cross join max_loaded m
    where s.snapshot_at > m.max_snapshot_at

),

prior_last_per_repo as (

    {% if is_incremental() %}
    select *
    from (
        select
            g.*,
            row_number() over (
                partition by repo_id
                order by snapshot_at desc
            ) as rn
        from {{ this }} g
    ) x
    where rn = 1
    {% else %}
    select *
    from (
        select
            cast(null as varchar) as _batch_id,
            cast(null as timestamp) as snapshot_at,
            cast(null as bigint) as repo_id,
            cast(null as varchar) as repo_scd_id,
            cast(null as varchar) as owner,
            cast(null as bigint) as stargazers_count,
            cast(null as bigint) as forks_count,
            cast(null as bigint) as watchers_count,
            cast(null as bigint) as open_issues_count,
            cast(null as timestamp) as pushed_at,
            cast(null as bigint) as prev_stargazers_count,
            cast(null as bigint) as prev_forks_count,
            cast(null as bigint) as prev_watchers_count,
            cast(null as bigint) as delta_stars,
            cast(null as bigint) as delta_forks,
            cast(null as bigint) as delta_watchers,
            cast(null as integer) as activity_flag,
            cast(null as double) as stars_points,
            cast(null as double) as forks_points,
            cast(null as double) as watchers_points,
            cast(null as double) as activity_points,
            cast(null as double) as popularity_rise_score,
            cast(null as varchar) as main_driver,
            cast(0 as integer) as is_new,
            cast(null as integer) as rn
    ) z
    where 1 = 0
    {% endif %}

),

calc_input as (

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
        pushed_at,
        is_new
    from new_rows

    union all

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
        pushed_at,
        0 as is_new
    from prior_last_per_repo

),

with_lags as (

    select
        *,
        lag(stargazers_count) over (partition by repo_id order by snapshot_at) as prev_stargazers_count,
        lag(forks_count) over (partition by repo_id order by snapshot_at) as prev_forks_count,
        lag(watchers_count) over (partition by repo_id order by snapshot_at) as prev_watchers_count
    from calc_input

),

features as (

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
        pushed_at,

        coalesce(prev_stargazers_count, stargazers_count) as prev_stargazers_count,
        coalesce(prev_forks_count, forks_count) as prev_forks_count,
        coalesce(prev_watchers_count, watchers_count) as prev_watchers_count,

        greatest(stargazers_count - coalesce(prev_stargazers_count, stargazers_count), 0) as delta_stars,
        greatest(forks_count - coalesce(prev_forks_count, forks_count), 0) as delta_forks,
        greatest(watchers_count - coalesce(prev_watchers_count, watchers_count), 0) as delta_watchers,

        case when pushed_at >= (snapshot_at - interval '1' day) then 1 else 0 end as activity_flag,
        is_new
    from with_lags

),

scored as (

    select
        *,
        (0.65 * delta_stars) as stars_points,
        (0.20 * delta_forks) as forks_points,
        (0.10 * delta_watchers) as watchers_points,
        (0.05 * activity_flag) as activity_points
    from features

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
    pushed_at,

    prev_stargazers_count,
    prev_forks_count,
    prev_watchers_count,

    delta_stars,
    delta_forks,
    delta_watchers,
    activity_flag,

    stars_points,
    forks_points,
    watchers_points,
    activity_points,

    (stars_points + forks_points + watchers_points + activity_points) as popularity_rise_score,

    case
        when stars_points >= forks_points and stars_points >= watchers_points and stars_points >= activity_points then 'star_growth'
        when forks_points >= watchers_points and forks_points >= activity_points then 'fork_growth'
        when watchers_points >= activity_points then 'watcher_growth'
        else 'recent_push_activity'
    end as main_driver
from scored
where is_new = 1