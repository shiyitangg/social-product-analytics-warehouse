{{
    config(
        materialized='incremental',
        unique_key='event_date',
        incremental_strategy='delete+insert',
        indexes=[{'columns': ['user_id', 'event_date']}]
    )
}}
-- Grain: one row per active user per day. Feeds DAU, creation rate and retention without rescanning fct_events.
select
    event_date,
    user_id,
    count(*)                                                          as events,
    count(*) filter (where event_type = 'app_open')                   as app_opens,
    count(*) filter (where event_type = 'content_view')               as content_views,
    count(*) filter (where event_type in ('like','comment','share','follow')) as interactions_made,
    count(*) filter (where event_type = 'content_create')             as posts_created
from {{ ref('fct_events') }}
{% if is_incremental() %}
where event_date >= {{ incremental_start_date(this) }}
{% endif %}
group by event_date, user_id
