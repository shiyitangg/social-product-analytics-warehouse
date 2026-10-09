{#
  SQL expression for the first event_date an incremental fact model must (re)build.
  - backfill:    --vars '{reprocess_from_date: "YYYY-MM-DD"}'
  - normal run:  latest event_date already in the target minus `lookback_days`
  Because the target is replaced one whole event_date at a time (delete+insert on event_date),
  re-running any window is idempotent.
#}
{% macro incremental_start_date(relation) -%}
    {%- if var('reprocess_from_date', none) is not none -%}
        date '{{ var("reprocess_from_date") }}'
    {%- else -%}
        (select coalesce(max(event_date), date '1900-01-01') from {{ relation }}) - {{ var('lookback_days') }}
    {%- endif -%}
{%- endmacro %}
