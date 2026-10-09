# Design notes

All data is **synthetic**. This document explains why the pipeline is built the way it is, what each choice
costs, and what broke while building it. Numbers come from running this repository (see README).

## 1. Problem shape

A daily batch of social-app events arrives in object storage. Four things make that hard to get right:

| Property | Injected rate | Consequence if ignored |
|---|---|---|
| Late arrival (1-3 days after the event) | about 3% of events | Daily counts change after the fact; alerts fire on incomplete days |
| Duplicate delivery of the same `event_id` | about 0.5% | Inflated engagement |
| Malformed rows (null user, unknown type, impossible timestamp) | about 0.2% | Broken joins, silent bad metrics |
| Slowly changing user attributes | 70,000 users produce 102,641 versions | Historical facts attributed to the wrong country/device/segment |

Because the generator knows exactly what it injected, correctness is checked against ground truth instead of
being asserted.

## 2. Lake: idempotent by construction (PySpark)

For ingest date D the job validates rows, writes the batch's de-duplicated valid rows to
`events_by_ingest/ingest_date=D` (overwrite), then **rebuilds** every `clean_events/event_date=X` partition
that D can touch from the `events_by_ingest` partitions in `[D - max_lag, D + max_lag]`, keeping the first
delivery of each `event_id`.

- *Why rebuild instead of append?* Appending late rows needs "have I seen this already?" state. Rebuilding a
  partition from its inputs makes re-runs, retries and out-of-order processing converge to the same result.
- *Why the window extends forward as well as backward?* Rows for event date X arrive on ingest dates X to
  X + max_lag. Reading only "everything before D" would make the result depend on processing order.
- *Cost:* each run rereads up to `2 x max_lag + 1` small ingest partitions. Acceptable at this scale; at larger
  scale this is where a table format with merge support (Iceberg/Delta) would replace hand-rolled partition rewrites.
- **Validation** is a list of ordered rules (`src/rules.py`); the first match becomes `reject_reason`.
  Rejected rows go to `quarantine_events`, never silently dropped.
- **Reject-rate gate:** if more than 1% of a batch is rejected the job fails before writing anything to the lake.
  Row-level rules handle expected noise; the gate handles "the upstream feed is broken".
- **Watermark** is derived from per-date done-markers (latest date such that every earlier date is done), so a
  gap cannot be skipped over.

## 3. Loader: only reload what changed

Each lake partition gets a fingerprint: row count plus an order-independent hash sum. A partition is reloaded
only if it is new or the fingerprint differs from `ops.loaded_partitions`. A reload runs in one transaction
(delete old rows, insert from a staging table, record the fingerprint), and counts are verified per partition.

- *Why fingerprints, not "load new dates only"?* A late event changes an **old** partition. Date-based
  incremental loading would miss it; content-based detection does not.
- *Limit:* a hash sum is a change detector, not a cryptographic guarantee. Collisions are astronomically
  unlikely for this use but not impossible.

## 4. dbt: modelling choices

**Incremental facts.** `fct_events`, `fct_content_engagement`, `fct_creator_daily_metrics` use
`delete+insert` keyed on `event_date`. Each run recomputes the trailing `lookback_days` (6 = max lag 5 + 1)
plus anything new; `reprocess_from_date` rebuilds from a chosen date for backfill. This handles late events and
leaves old days alone. The sandbox experiments (a mini dbt-compatible runner) showed incremental,
catch-up and backfill paths producing output identical to a full rebuild; on the real stack the backfill drill
left `fct_events` at exactly the same row count.

**SCD Type 2 `dim_users`.** Built from daily snapshots: a new version starts when country, device or segment
differs from the previous snapshot. `valid_to` is exclusive, version 1 starts at `least(signup_date, first_seen)`
so signup-day events resolve, and facts join *as of* the event date. A singular test asserts that versions are
contiguous, and a monitoring check fails if any user lacks exactly one current version.
*Limitation:* changes are visible only at snapshot (daily) granularity.

**Complete vs. provisional days.** `complete_through = latest ingest date - max_lag_days`. Days after it can still
receive late events, so retention, anomaly detection and alerts exclude them. Without this rule an alert would
fire on a day that merely has not finished arriving.

**Custom generic tests** (`unique_combination`, `value_between`, `null_rate_below`) cover rules that dbt's four
built-ins cannot express. Five singular tests check cross-model invariants, for example
`fct_events` per-day counts equal raw valid counts, and no alert is raised on an incomplete day.

## 5. Quality gates: four layers, each with a different job

1. **Row rules** (Spark): per-row validity, quarantined with a reason.
2. **Batch gate** (Spark): reject rate above 1% stops the pipeline before any clean data is written to the lake.
3. **dbt tests** (83): structural and business invariants on the modelled data.
4. **Publish gate** (Airflow task): consumers read `published.*`, which is replaced in one transaction **only if**
   `monitoring.mon_data_quality_checks` has no `fail`. A broken run therefore leaves yesterday's numbers in place.

Observed in the drills: the 5.89% bad batch is stopped at layer 2, and `published` keeps serving the previous day.

## 6. Orchestration (Airflow)

`check_raw_landed -> spark_incremental -> load_postgres -> dbt_seed -> dbt_run -> dbt_test -> publish -> notify_new_alerts`

- The logical date is the **ingest date**; every task is idempotent, so retries, clears and backfills are safe.
- Retries: 2 by default, 3 on the landing check, 1 on Spark (a gate failure is deterministic, one retry covers
  infrastructure blips), **0 on `dbt_test`** (retrying a failed quality gate would only hide it).
- `max_active_runs=1`: ingest dates share a watermark and must be processed one at a time.
- Failures are written to `ops.pipeline_alerts`. A `BashOperator` only reports "exit code 1", so the failure
  callback also extracts the real reason from the task log (for example the `GateFailure ... reject rate 5.89%`
  line). Slack posting is optional (`SLACK_WEBHOOK_URL`) and untested.
- New creator alerts are queued in `ops.alert_outbox`, which is what makes notification idempotent.
- The DAG ships paused and is exercised through `airflow dags backfill`, because the demo data ends in
  September 2026 and a daily schedule has nothing new to process.

## 7. Evaluating the alert honestly

The planted declines live in a dbt seed read only by `evaluation.*` models; the production models never see it.
Outcomes are `true_positive`, `early_alert` (counted as false alarm), `false_positive` and `missed`.

The threshold (ratio 0.5) was chosen while looking at the same data it is scored on, so the reported
precision 0.857 / recall 0.800 are optimistic (README has the sensitivity table: ratio 0.6 gives precision 0.400).
A real evaluation would tune on one period and report on a later one.

## 8. Problems hit while building, and fixes

| Problem | Cause | Fix |
|---|---|---|
| Creators showed zero engagement on many days | Generator drew views only from content under a week old | Added a long-lived catalogue (items stay eligible 90 days, evergreen weight) |
| `dbt seed` said the Postgres adapter was unsupported | `pip install dbt-postgres==1.8.2` pulled a 2.x dbt-core | Pinned `dbt-core~=1.10.0` and `dbt-postgres~=1.10.0` |
| Airflow DAG: `No such option '--project-dir'` | Global dbt options placed before the subcommand | Subcommand first: `dbt run --project-dir ...` |
| `dbt run` failed in Airflow only: `could not resize shared memory segment` | Docker gives containers 64 MB `/dev/shm`; parallel query plans on the 10M-row join needed more | `shm_size: 512mb` and `max_parallel_workers_per_gather=0` on the Postgres service |
| "Retry drill" never failed | Keyed on Airflow's attempt counter, which keeps growing across re-runs | Per-run marker file decides the first attempt |
| Failure alert said only "exit code 1" | `BashOperator` hides the command's output | Callback reads the cause from the task log |
| Occasional `DeadlockDetected` in Airflow's own metadata DB during backfill | Scheduler and task process updating the same `dag_run` row | Airflow retried and recovered on its own; left as is, noted for honesty |

## 9. What would change at real scale

- Table format with ACID merge (Iceberg/Delta) instead of rewriting partitions by hand.
- A warehouse with partitioned/clustered storage instead of one PostgreSQL instance; the `delete+insert` by
  date pattern carries over directly.
- Airflow with a distributed executor and separate scheduler/web services; real secrets management.
- Streaming or micro-batch ingestion if latency matters; the late-arrival and completeness rules would still apply.
- Alert thresholds fitted per creator segment and validated on held-out time periods, with seasonality.
- Data contracts with the upstream producer so schema drift is rejected at the edge.
