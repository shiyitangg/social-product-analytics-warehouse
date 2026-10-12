-- Retention fact vs mart_retention: for every cohort date in the mart, cohort sizes and retained counts must match.
select m.cohort_date, m.cohort_size, b.cohort_size as bi_cohort_size, m.retained_d1, b.retained_d1 as bi_retained_d1,
       m.retained_d7, b.retained_d7 as bi_retained_d7
from {{ ref('mart_retention') }} m
join (
    select cohort_date, sum(cohort_size) as cohort_size, sum(retained_d1) as retained_d1, sum(retained_d7) as retained_d7
    from {{ ref('bi_retention_cohorts') }}
    group by cohort_date
) b using (cohort_date)
where m.cohort_size <> b.cohort_size or m.retained_d1 <> b.retained_d1 or m.retained_d7 <> b.retained_d7
