{% snapshot snap_repo_scd2 %}

{{
    config(
        target_schema='snapshots',
        unique_key='repo_id',
        strategy='check',
        check_cols=[
            'full_name',
            'description',
            'owner',
            'language',
            'is_archived',
            'is_fork'
        ]
    )
}}

select
    repo_id,
    full_name,
    description,
    owner,
    language,
    is_archived,
    is_fork,
    _ingested_at
from {{ ref('stg_repos') }}

{% endsnapshot %}