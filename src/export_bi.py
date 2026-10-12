"""Export the dashboard-serving layer (schema `bi`) to CSV files for Power BI / Tableau / any BI tool.

    make export-bi          (runs inside the `runner` container; files appear in ./bi/export/)

Writes one CSV per table in the `bi` schema plus `manifest.json` with row counts, the quality-check tally and a few
reference numbers. After building the dashboard, the same numbers must appear in its KPI cards: that is the
acceptance test for the dashboard.

The export is NOT blocked by failing quality checks: the dashboard has a data-quality page, and a red status there
is information. The failing-check count is stored in the manifest and shown on the dashboard.
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

WAREHOUSE = dict(
    host=os.getenv("WAREHOUSE_HOST", "localhost"),
    port=int(os.getenv("WAREHOUSE_PORT", "5433")),
    dbname=os.getenv("WAREHOUSE_DB", "warehouse"),
    user=os.getenv("WAREHOUSE_USER", "wh"),
    password=os.getenv("WAREHOUSE_PASSWORD", "wh"),
)

# Reference numbers written to the manifest (all computed from the bi tables themselves).
KEY_VALUES = {
    "latest_complete_day": "select complete_through::text from bi.bi_freshness",
    "dau_on_latest_complete_day": (
        "select sum(active_users) from bi.bi_daily_activity "
        "where event_date = (select complete_through from bi.bi_freshness)"),
    "posts_created_complete_days": "select sum(posts_created) from bi.bi_daily_activity where not is_provisional",
    "d1_retention_overall": (
        "select round(sum(retained_d1)::numeric / nullif(sum(d1_eligible), 0), 4) from bi.bi_retention_cohorts"),
    "d7_retention_overall": (
        "select round(sum(retained_d7)::numeric / nullif(sum(d7_eligible), 0), 4) from bi.bi_retention_cohorts"),
    "latest_full_week_start": "select max(week_start_date)::text from bi.bi_weekly_active where is_full_week",
    "wau_latest_full_week": (
        "select sum(wau) from bi.bi_weekly_active "
        "where is_full_week and week_start_date = (select max(week_start_date) from bi.bi_weekly_active where is_full_week)"),
    "interactions_per_view_complete_days": (
        "select round(sum(interactions)::numeric / nullif(sum(content_views), 0), 5) "
        "from bi.bi_daily_activity where not is_provisional"),
    "creators_alerted": "select count(*) from bi.bi_creator_alerts",
    "creators_alerted_critical": "select count(*) from bi.bi_creator_alerts where severity = 'critical'",
    "quarantined_rows_total": "select sum(quarantined_rows) from bi.bi_dq_ingest_daily",
    "quarantine_rate_overall": (
        "select round(sum(quarantined_rows)::numeric / nullif(sum(rows_in), 0), 5) from bi.bi_dq_ingest_daily"),
    "hours_since_last_load": "select round(hours_since_last_load::numeric, 1) from bi.bi_freshness",
}


def select_list(columns: list[tuple[str, str]]) -> str:
    """SELECT list for one table. Booleans are written as 'true'/'false' (Postgres COPY would write 't'/'f')."""
    parts = []
    for name, dtype in columns:
        q = f'"{name}"'
        parts.append(f"{q}::text as {q}" if dtype == "boolean" else q)
    return ", ".join(parts)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="bi/export")
    a = ap.parse_args(argv)
    import psycopg2

    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    conn = psycopg2.connect(**WAREHOUSE)
    try:
        cur = conn.cursor()
        cur.execute("select table_name from information_schema.tables where table_schema = 'bi' order by table_name")
        tables = [r[0] for r in cur.fetchall()]
        if not tables:
            raise SystemExit("no tables in schema `bi`: run `make dbt-run` first")
        manifest = {"exported_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "tables": {}}
        for t in tables:
            cur.execute("select column_name, data_type from information_schema.columns "
                        "where table_schema = 'bi' and table_name = %s order by ordinal_position", (t,))
            cols = cur.fetchall()
            order_by = ", ".join(str(i + 1) for i in range(min(3, len(cols))))
            sql = f"copy (select {select_list(cols)} from bi.{t} order by {order_by}) to stdout with (format csv, header true)"
            with open(out / f"{t}.csv", "w", newline="") as fh:
                cur.copy_expert(sql, fh)
            cur.execute(f"select count(*) from bi.{t}")
            manifest["tables"][t] = int(cur.fetchone()[0])
            print(f"  {t:<28} {manifest['tables'][t]:>8} rows")
        cur.execute("select checks_pass, checks_warn, checks_fail from bi.bi_freshness")
        p, w, f = cur.fetchone()
        manifest["quality_checks"] = {"pass": int(p), "warn": int(w), "fail": int(f)}
        manifest["key_values"] = {}
        for k, sql in KEY_VALUES.items():
            cur.execute(sql)
            v = cur.fetchone()[0]
            manifest["key_values"][k] = float(v) if hasattr(v, "as_tuple") else v
        (out / "manifest.json").write_text(json.dumps(manifest, indent=2, default=str))
        print(f"\nexported {len(tables)} tables to {out}/")
        if manifest["quality_checks"]["fail"]:
            print(f"WARNING: {manifest['quality_checks']['fail']} data-quality check(s) are failing; "
                  "the dashboard's data-quality page will show it.")
        print("reference numbers (the dashboard's KPI cards must show the same):")
        for k, v in manifest["key_values"].items():
            print(f"  {k:<38} {v}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
