{# Generic tests (no external packages needed). A dbt test FAILS when its query returns any rows. #}

{% test unique_combination(model, columns) %}
select {{ columns | join(', ') }}, count(*) as n_rows
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}

{% test value_between(model, column_name, min_value=none, max_value=none) %}
select {{ column_name }}
from {{ model }}
where {{ column_name }} is not null
  and (
      {% if min_value is not none %}{{ column_name }} < {{ min_value }}{% else %}false{% endif %}
      or
      {% if max_value is not none %}{{ column_name }} > {{ max_value }}{% else %}false{% endif %}
  )
{% endtest %}

{# Fails when the share of NULLs in the column exceeds max_rate (optionally only among rows matching `where_sql`). #}
{% test null_rate_below(model, column_name, max_rate, where_sql='true') %}
select null_rate
from (
    select (count(*) filter (where {{ column_name }} is null))::numeric / nullif(count(*), 0) as null_rate
    from {{ model }}
    where {{ where_sql }}
) t
where null_rate > {{ max_rate }}
{% endtest %}
