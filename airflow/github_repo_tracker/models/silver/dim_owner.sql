{{
    config(
        materialized='table'
    )
}}

with ranked as (
    select
        owner,
        row_number() over (
            partition by owner
            order by _ingested_at desc
        ) as rn
    from {{ ref('stg_repos') }}
    where owner is not null
)

select
    owner
from ranked
where rn = 1