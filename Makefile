.PHONY: build up reset upload ingest verify idempotency load dbt-seed dbt-run dbt-test dbt-freshness dbt-docs dbt-backfill airflow-up airflow-logs airflow-check airflow-run airflow-run-flaky airflow-backfill batch-bad batch-good sql test-spark down

RUN  = docker compose run --rm runner
PY   = /opt/venvs/spark/bin/python
DBT  = /opt/venvs/dbt/bin/dbt
DBTA = --project-dir dbt --profiles-dir dbt

build:
	docker compose build runner
up:
	docker compose up -d minio postgres
# wipe object storage + warehouse volumes (needed after the data changes)
reset:
	docker compose down -v

# --- stage 2: raw -> lake (PySpark) ---
upload:
	$(RUN) $(PY) src/upload_raw.py
ingest:
	$(RUN) $(PY) src/spark_incremental.py --mode incremental
verify:
	$(RUN) $(PY) src/verify_lake.py
idempotency:
	$(RUN) $(PY) src/idempotency_test.py

# --- stage 3: lake -> PostgreSQL -> dbt ---
load:
	$(RUN) $(PY) src/load_postgres.py
dbt-seed:
	$(RUN) $(DBT) seed $(DBTA)
dbt-run:
	$(RUN) $(DBT) run $(DBTA)
dbt-test:
	$(RUN) $(DBT) test $(DBTA)
dbt-freshness:
	$(RUN) $(DBT) source freshness $(DBTA)
dbt-docs:
	$(RUN) $(DBT) docs generate --static $(DBTA)
# backfill facts from a date:  make dbt-backfill FROM=2026-08-10
dbt-backfill:
	$(RUN) $(DBT) run $(DBTA) --vars '{reprocess_from_date: "$(FROM)"}'

# --- stage 4: Airflow ---
airflow-up:
	docker compose up -d airflow
airflow-logs:
	docker compose logs --tail 60 airflow
airflow-check:
	docker compose exec airflow airflow dags list-import-errors
# The DAG is left PAUSED on purpose (the demo data ends in mid-September), so runs are started with `backfill`,
# which honours retries, failure alerts and the quality gate exactly like a scheduled run.
#   make airflow-run DATE=2026-09-10
airflow-run:
	docker compose exec airflow airflow dags backfill social_warehouse_daily -s $(DATE) -e $(DATE) --reset-dagruns -y
# first attempt of check_raw_landed fails on purpose, the retry succeeds
airflow-run-flaky:
	docker compose exec airflow airflow dags backfill social_warehouse_daily -s $(DATE) -e $(DATE) --reset-dagruns -y -c '{"simulate_transient_failure": true}'
# rebuild facts from the first date of a range:  make airflow-backfill FROM=2026-08-10 TO=2026-08-12
airflow-backfill:
	docker compose exec airflow airflow dags backfill social_warehouse_daily -s $(FROM) -e $(TO) --reset-dagruns -y -c '{"reprocess": true}'
batch-bad:
	$(RUN) $(PY) src/make_batch.py --kind bad
batch-good:
	$(RUN) $(PY) src/make_batch.py --kind good

sql:
	docker compose exec postgres psql -P pager=off -U wh -d warehouse

test-spark:
	$(RUN) $(PY) -m pytest -q tests
down:
	docker compose down
