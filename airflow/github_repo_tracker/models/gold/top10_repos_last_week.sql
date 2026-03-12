{{ config(materialized='table') }}

with windowed as (
    select *
    from {{ ref('fact_repo_growth') }}
    where snapshot_at >= current_timestamp - interval '7' day
),

agg as (
    select
        repo_id,
        max_by(repo_scd_id, snapshot_at) as repo_scd_id,
        max_by(owner, snapshot_at) as owner,
        sum(delta_stars) as delta_stars,
        sum(delta_forks) as delta_forks,
        sum(delta_watchers) as delta_watchers,
        sum(stars_points) as stars_points,
        sum(forks_points) as forks_points,
        sum(watchers_points) as watchers_points,
        sum(activity_points) as activity_points
    from windowed
    group by repo_id
),

scored as (
    select
        a.*,
        (stars_points + forks_points + watchers_points + activity_points) as popularity_rise_score,
        case
            when stars_points >= forks_points and stars_points >= watchers_points and stars_points >= activity_points then 'star_growth'
            when forks_points >= watchers_points and forks_points >= activity_points then 'fork_growth'
            when watchers_points >= activity_points then 'watcher_growth'
            else 'recent_push_activity'
        end as main_driver
    from agg a
),

ranked as (
    select
        s.*,
        d.full_name,
        d.language,
        d.description,
        row_number() over (
            order by popularity_rise_score desc, delta_stars desc, delta_forks desc
        ) as rank_position
    from scored s
    left join {{ ref('dim_repository_scd2') }} d
      on s.repo_scd_id = d.repo_scd_id
)

select *
from ranked
where rank_position <= 10