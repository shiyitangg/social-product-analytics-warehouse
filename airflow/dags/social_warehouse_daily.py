"""Daily orchestration of the Social Product Analytics Warehouse (SYNTHETIC data).

    check_raw_landed -> spark_incremental -> load_postgres -> dbt_seed -> dbt_run -> dbt_test -> publish -> notify

* The run's logical date (ds) is the INGEST date being processed. Every step is idempotent, so re-running a
  date (retry, clear, backfill) never duplicates data.
* dbt_test is the quality gate. If any test fails nothing downstream runs, and `published.*` keeps serving the
  last good numbers. Failures are recorded in ops.pipeline_alerts (and Slack if SLACK_WEBHOOK_URL is set).
* Retries: each task retries twice with a short delay. Trigger with {"simulate_transient_failure": true} to
  watch check_raw_landed fail once and then succeed on its retry.
* Backfill old dates:  airflow dags backfill social_warehouse_daily -s 2026-08-10 -e 2026-08-12 -c '{"reprocess": true}'
  ("reprocess" tells dbt to rebuild the fact tables from that date instead of only the trailing window.)
"""
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/project/src")
import pipeline_tasks as T  # noqa: E402

PROJECT = "/opt/project"
SPARK_PY = "/opt/venvs/spark/bin/python"
DBT = "/opt/venvs/dbt/bin/dbt"
DBT_ARGS = "--project-dir dbt --profiles-dir dbt"

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(seconds=20),
    "on_failure_callback": T.record_failure,
}

REPROCESS = ("{% if (dag_run.conf or {}).get('reprocess') %}"
             "--vars '{reprocess_from_date: \"{{ ds }}\"}'{% endif %}")


def _check_raw(ds=None, params=None, ti=None, dag_run=None, **_):
    conf = (dag_run.conf or {}) if dag_run else {}
    simulate = bool(conf.get("simulate_transient_failure"))
    first_attempt_of_run = True
    if simulate:
        # Airflow's try_number keeps counting across cleared/re-run runs, so remember per run with a marker file:
        # the first attempt of THIS run fails, its retry finds the marker and succeeds.
        stamp = int((dag_run.start_date or dag_run.execution_date).timestamp())
        marker = f"/tmp/flaky_{ds}_{stamp}"
        first_attempt_of_run = not os.path.exists(marker)
        if first_attempt_of_run:
            open(marker, "w").close()
    T.check_raw_landed(ds, simulate, 1 if first_attempt_of_run else 2)


def _publish(ds=None, run_id=None, **_):
    T.publish(ds, run_id)


def _notify(**_):
    T.notify_new_alerts()


with DAG(
    dag_id="social_warehouse_daily",
    description="raw -> Spark incremental lake -> PostgreSQL -> dbt models + tests -> publish -> alerts (synthetic data)",
    start_date=datetime(2026, 8, 1),
    schedule="@daily",
    catchup=False,              # use `airflow dags backfill` for specific ranges; 40 sequential full runs is slow
    max_active_runs=1,          # ingest dates must be processed one at a time (shared watermark)
    default_args=default_args,
    tags=["warehouse", "synthetic"],
) as dag:

    check_raw_landed = PythonOperator(task_id="check_raw_landed", python_callable=_check_raw, retries=3)

    spark_incremental = BashOperator(
        task_id="spark_incremental",
        bash_command=f"cd {PROJECT} && {SPARK_PY} src/spark_incremental.py --mode dates --ingest-date {{{{ ds }}}}",
        retries=1,               # a quality-gate failure is deterministic; one retry covers a transient infra error
        execution_timeout=timedelta(minutes=45),
    )

    load_postgres = BashOperator(
        task_id="load_postgres",
        bash_command=f"cd {PROJECT} && {SPARK_PY} src/load_postgres.py",
        execution_timeout=timedelta(minutes=45),
    )

    dbt_seed = BashOperator(task_id="dbt_seed", bash_command=f"cd {PROJECT} && {DBT} seed {DBT_ARGS}")

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {PROJECT} && {DBT} run {DBT_ARGS} " + REPROCESS,
        execution_timeout=timedelta(minutes=45),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {PROJECT} && {DBT} test {DBT_ARGS}",
        retries=0,               # tests are deterministic: retrying a failed quality gate would only hide it
        execution_timeout=timedelta(minutes=30),
    )

    publish = PythonOperator(task_id="publish", python_callable=_publish)
    notify_new_alerts = PythonOperator(task_id="notify_new_alerts", python_callable=_notify)

    (check_raw_landed >> spark_incremental >> load_postgres >> dbt_seed >> dbt_run >> dbt_test
     >> publish >> notify_new_alerts)
