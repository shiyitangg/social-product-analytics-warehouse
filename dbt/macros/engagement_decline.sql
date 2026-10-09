{#
  Scores a dense daily series (one row per entity per day, zeros filled in) for a sustained drop.
  Output columns: entity, metric_date, value, recent_avg, recent_max, recent_n, baseline_avg, baseline_n, is_decline.
  is_decline = the baseline is complete, big enough, and EVERY one of the last `recent_days` values is
  <= ratio x baseline_avg. Requiring every recent day (not just their mean) keeps one-day dips from alerting.
#}
{% macro engagement_decline_scored(series, entity, date_col, value_col, recent_days, baseline_days, min_baseline, ratio) -%}
select
    {{ entity }}  as entity_id,
    {{ date_col }} as metric_date,
    {{ value_col }} as value,
    recent_avg, recent_max, recent_n, baseline_avg, baseline_n,
    (baseline_n = {{ baseline_days }}
     and recent_n = {{ recent_days }}
     and baseline_avg >= {{ min_baseline }}
     and recent_max <= {{ ratio }} * baseline_avg) as is_decline
from (
    select
        {{ entity }}, {{ date_col }}, {{ value_col }},
        avg({{ value_col }})   over w_recent as recent_avg,
        max({{ value_col }})   over w_recent as recent_max,
        count(*)               over w_recent as recent_n,
        avg({{ value_col }})   over w_base   as baseline_avg,
        count(*)               over w_base   as baseline_n
    from {{ series }}
    window
        w_recent as (partition by {{ entity }} order by {{ date_col }}
                     rows between {{ recent_days - 1 }} preceding and current row),
        w_base   as (partition by {{ entity }} order by {{ date_col }}
                     rows between {{ recent_days + baseline_days - 1 }} preceding and {{ recent_days }} preceding)
) scored
{%- endmacro %}
