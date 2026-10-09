"""Small, testable task functions used by the Airflow DAG (airflow/dags/social_warehouse_daily.py).

They run in Airflow's own Python (psycopg2 + boto3 are bundled with the Airflow image), while the heavy steps
(Spark, dbt) run in their own virtualenvs through BashOperator.
"""
import json
import os
import urllib.request
from datetime import datetime, timezone

WAREHOUSE = dict(
    host=os.getenv("WAREHOUSE_HOST", "localhost"), port=int(os.getenv("WAREHOUSE_PORT", "5432")),
    dbname=os.getenv("WAREHOUSE_DB", "warehouse"), user=os.getenv("WAREHOUSE_USER", "wh"),
    password=os.getenv("WAREHOUSE_PASSWORD", "wh"),
)

# Marts and monitoring tables that consumers read from the `published` schema.
PUBLISH_TABLES = [
    ("marts", "mart_daily_growth"),
    ("marts", "mart_retention"),
    ("marts", "mart_creator_engagement_alerts"),
    ("marts", "mart_creator_alert_summary"),
    ("marts", "mart_community_engagement_alerts"),
    ("monitoring", "mon_pipeline_state"),
    ("monitoring", "mon_data_quality_checks"),
    ("monitoring", "mon_ingest_quality"),
    ("monitoring", "mon_metric_anomalies"),
]

DDL = """
create schema if not exists ops;
create schema if not exists published;
create table if not exists ops.publish_log (
    id bigserial primary key, dag_run_id text, ingest_date date, published_at timestamptz not null default now(),
    tables_published int, checks_total int, checks_warn int
);
create table if not exists ops.pipeline_alerts (
    id bigserial primary key, created_at timestamptz not null default now(), dag_id text, task_id text,
    dag_run_id text, ingest_date date, try_number int, error text
);
create table if not exists ops.alert_outbox (
    creator_id bigint primary key, first_alert_date date not null, drop_pct numeric, severity text,
    baseline_events_per_day numeric, queued_at timestamptz not null default now(), delivered boolean not null default false
);
"""


def connect():
    import psycopg2
    return psycopg2.connect(**WAREHOUSE)


def ensure_tables(conn):
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()


# ---------------------------------------------------------------- 1. landing check
def check_raw_landed(ingest_date: str, simulate_transient_failure: bool = False, try_number: int = 1) -> int:
    """Fail (so Airflow retries) if the day's raw partition is not in object storage yet.
    `simulate_transient_failure` makes the FIRST attempt fail like a flaky network, to demonstrate retries."""
    if simulate_transient_failure and try_number == 1:
        raise ConnectionError("simulated transient object-storage timeout (first attempt only)")
    import common as C
    s3 = C.s3_client()
    resp = s3.list_objects_v2(Bucket=C.RAW_BUCKET, Prefix=f"events/ingest_date={ingest_date}/")
    n = resp.get("KeyCount", 0)
    if n == 0:
        raise FileNotFoundError(f"raw events for ingest_date={ingest_date} have not landed in s3://{C.RAW_BUCKET}/")
    print(f"raw landed: {n} file(s) for ingest_date={ingest_date}")
    return n


# ---------------------------------------------------------------- 2. publish
def read_checks(conn):
    with conn.cursor() as cur:
        cur.execute("select check_name, observed, warn_above, fail_above, status "
                    "from monitoring.mon_data_quality_checks order by check_name")
        return cur.fetchall()


def publish(ingest_date: str, dag_run_id: str = "manual") -> dict:
    """Copy the marts into `published.*` in ONE transaction - but only if no quality check is in 'fail'.
    Consumers read `published`, so a failed run can never expose half-built or unvalidated numbers."""
    conn = connect()
    try:
        ensure_tables(conn)
        checks = read_checks(conn)
        for name, observed, warn, fail, status in checks:
            print(f"  {status.upper():<5} {name:<52} observed={observed} warn>{warn} fail>{fail}")
        failed = [c[0] for c in checks if c[4] == "fail"]
        if failed or not checks:
            raise RuntimeError(f"quality gate blocked publication: failed={failed or 'no checks found'}")
        with conn.cursor() as cur:
            for schema, table in PUBLISH_TABLES:
                cur.execute(f"drop table if exists published.{table}")
                cur.execute(f"create table published.{table} as select * from {schema}.{table}")
            cur.execute(
                "insert into ops.publish_log(dag_run_id, ingest_date, tables_published, checks_total, checks_warn) "
                "values (%s, %s, %s, %s, %s)",
                (dag_run_id, ingest_date, len(PUBLISH_TABLES), len(checks), sum(1 for c in checks if c[4] == "warn")))
        conn.commit()
        print(f"published {len(PUBLISH_TABLES)} tables to schema `published` for ingest_date={ingest_date}")
        return {"tables": len(PUBLISH_TABLES), "checks": len(checks)}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------- 3. business alerts
def notify_new_alerts() -> list:
    """Queue creator engagement-decline alerts that have not been announced yet (idempotent via ops.alert_outbox).
    If SLACK_WEBHOOK_URL is set the message is also POSTed there; otherwise it is only logged."""
    conn = connect()
    try:
        ensure_tables(conn)
        with conn.cursor() as cur:
            cur.execute("""
                select s.creator_id, s.first_alert_date, s.drop_pct_at_alert, s.severity, s.baseline_events_per_day
                from published.mart_creator_alert_summary s
                left join ops.alert_outbox o using (creator_id)
                where o.creator_id is null
                order by s.first_alert_date, s.creator_id""")
            new = cur.fetchall()
            for r in new:
                cur.execute("insert into ops.alert_outbox(creator_id, first_alert_date, drop_pct, severity, "
                            "baseline_events_per_day, delivered) values (%s,%s,%s,%s,%s,%s)",
                            (*r, bool(os.getenv("SLACK_WEBHOOK_URL"))))
        conn.commit()
    finally:
        conn.close()
    for creator_id, d, drop, sev, base in new:
        text = (f"[{sev}] creator {creator_id}: engagement down {float(drop):.0%} vs prior 14-day baseline "
                f"({float(base):.0f}/day), first flagged {d}")
        print("ALERT", text)
        hook = os.getenv("SLACK_WEBHOOK_URL")
        if hook:
            req = urllib.request.Request(hook, data=json.dumps({"text": text}).encode(),
                                         headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=10)
    print(f"{len(new)} new creator alert(s) queued")
    return new


# ---------------------------------------------------------------- 4. failure alerting
REASON_PATTERNS = ("GateFailure", "Failure in model", "Failure in test", "Database Error", "Compilation Error")


def log_reason(ti, max_lines: int = 3) -> str:
    """Best effort: pull the real cause out of the task log. A BashOperator only reports 'exit code 1' as its
    exception, but the reason (quality gate, SQL error, failing test) is in the command output."""
    import glob
    base = os.getenv("AIRFLOW__LOGGING__BASE_LOG_FOLDER", "/opt/airflow/logs")
    pattern = f"{base}/dag_id={ti.dag_id}/run_id={ti.run_id}/task_id={ti.task_id}/attempt=*.log"
    try:
        files = sorted(glob.glob(pattern), key=os.path.getmtime)
        if not files:
            return ""
        found = []
        with open(files[-1], errors="replace") as fh:
            for line in fh:
                if any(p in line for p in REASON_PATTERNS):
                    msg = line.split(" - ", 1)[-1].strip()
                    if "GateFailure" in msg:          # drop Spark's progress-bar prefix
                        msg = msg[msg.index("GateFailure"):]
                    if msg not in found:
                        found.append(msg)
        return " | ".join(found[:max_lines])
    except Exception:  # noqa: BLE001 - diagnostics must never break alerting
        return ""


def record_failure(context) -> None:
    """Airflow on_failure_callback: persist the failure so it is visible in SQL (and optionally Slack)."""
    ti = context["task_instance"]
    err = str(context.get("exception"))
    reason = log_reason(ti)
    if reason:
        err = f"{reason}  [{err}]"
    err = err[:2000]
    try:
        conn = connect()
        ensure_tables(conn)
        with conn.cursor() as cur:
            cur.execute("insert into ops.pipeline_alerts(dag_id, task_id, dag_run_id, ingest_date, try_number, error) "
                        "values (%s,%s,%s,%s,%s,%s)",
                        (ti.dag_id, ti.task_id, context["run_id"], context["ds"], ti.try_number, err))
        conn.commit()
        conn.close()
    except Exception as e:  # noqa: BLE001  - never mask the original failure
        print(f"could not persist failure alert: {e}")
    print(f"PIPELINE ALERT: {ti.dag_id}.{ti.task_id} failed (attempt {ti.try_number}) for {context['ds']}: {err}")
    hook = os.getenv("SLACK_WEBHOOK_URL")
    if hook:
        try:
            body = {"text": f":rotating_light: {ti.dag_id}.{ti.task_id} failed for {context['ds']}: {err[:300]}"}
            urllib.request.urlopen(urllib.request.Request(
                hook, data=json.dumps(body).encode(), headers={"Content-Type": "application/json"}), timeout=10)
        except Exception as e:  # noqa: BLE001
            print(f"slack post failed: {e}")


if __name__ == "__main__":  # manual use:  python src/pipeline_tasks.py publish 2026-09-09
    import sys
    cmd = sys.argv[1]
    if cmd == "publish":
        publish(sys.argv[2])
    elif cmd == "notify":
        notify_new_alerts()
    elif cmd == "check-raw":
        check_raw_landed(sys.argv[2])
