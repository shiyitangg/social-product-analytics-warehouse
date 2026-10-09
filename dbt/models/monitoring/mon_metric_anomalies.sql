{#
  Daily product-metric anomaly detection. Each metric is compared with its own trailing 7 COMPLETE days
  (z-score). A day is flagged only if BOTH |z| >= 3 AND the value differs from the trailing mean by at least 10%,
  so a statistically "significant" but operationally trivial wiggle in a very stable metric does not page anyone.
  Provisional days are skipped because late events have not all arrived.
#}
with unpivoted as (
    select g.event_date, m.metric_name, m.metric_value
    from {{ ref('mart_daily_growth') }} g
    cross join lateral (values
        ('dau',                   g.dau::numeric),
        ('events_total',          g.events_total::numeric),
        ('posts_created',         g.posts_created::numeric),
        ('content_creation_rate', g.content_creation_rate)
    ) as m(metric_name, metric_value)
    where not g.is_provisional
),

scored as (
    select
        event_date, metric_name, metric_value,
        avg(metric_value)         over w as trailing_mean,
        stddev_samp(metric_value) over w as trailing_sd,
        count(*)                  over w as trailing_n
    from unpivoted
    window w as (partition by metric_name order by event_date rows between 7 preceding and 1 preceding)
)

select
    event_date, metric_name, metric_value,
    round(trailing_mean, 4) as trailing_mean,
    round((metric_value - trailing_mean) / nullif(trailing_sd, 0), 2) as z_score,
    (trailing_n = 7
     and abs((metric_value - trailing_mean) / nullif(trailing_sd, 0)) >= 3
     and abs(metric_value / nullif(trailing_mean, 0) - 1) >= 0.10) as is_anomaly
from scored
order by metric_name, event_date
