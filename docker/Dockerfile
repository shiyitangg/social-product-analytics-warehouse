# One image for everything: Airflow (Stage 4) + isolated virtualenvs for Spark and dbt.
FROM apache/airflow:2.10.5-python3.11
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl procps \
 && rm -rf /var/lib/apt/lists/*

# Spark lives in its own venv so it cannot disturb Airflow's pinned dependencies.
RUN python -m venv /opt/venvs/spark \
 && /opt/venvs/spark/bin/pip install --no-cache-dir pyspark==3.5.3 boto3 pyarrow pandas pytest

# S3A (MinIO) + PostgreSQL JDBC jars matching Spark 3.5.3's bundled Hadoop 3.3.4.
RUN set -e; \
    JARS=$(/opt/venvs/spark/bin/python -c "import pyspark,os;print(os.path.join(os.path.dirname(pyspark.__file__),'jars'))"); \
    curl -fsSL -o $JARS/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar; \
    curl -fsSL -o $JARS/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar; \
    curl -fsSL -o $JARS/postgresql-42.7.4.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar

# dbt in its own venv as well (Stage 3).
RUN python -m venv /opt/venvs/dbt \
 && /opt/venvs/dbt/bin/pip install --no-cache-dir dbt-core~=1.10.0 dbt-postgres~=1.10.0

WORKDIR /opt/project
# Airflow refuses to run as root; the `runner` service overrides this with user: "0:0".
USER airflow
