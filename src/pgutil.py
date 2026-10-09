"""Run SQL on PostgreSQL through Spark's own JVM JDBC driver (no extra Python packages needed)."""
import os

HOST = os.getenv("WAREHOUSE_HOST", "localhost")
PORT = os.getenv("WAREHOUSE_PORT", "5432")
DB = os.getenv("WAREHOUSE_DB", "warehouse")
USER = os.getenv("WAREHOUSE_USER", "wh")
PASSWORD = os.getenv("WAREHOUSE_PASSWORD", "wh")
JDBC_URL = f"jdbc:postgresql://{HOST}:{PORT}/{DB}?reWriteBatchedInserts=true"


def connect(spark):
    jvm = spark.sparkContext._gateway.jvm
    jvm.java.lang.Class.forName("org.postgresql.Driver")
    return jvm.java.sql.DriverManager.getConnection(JDBC_URL, USER, PASSWORD)


def execute(conn, sql):
    st = conn.createStatement()
    try:
        st.execute(sql)
    finally:
        st.close()


def run_script(conn, text):
    """Execute a .sql file (statements separated by ';', '--' comment lines ignored)."""
    cleaned = "\n".join(l for l in text.splitlines() if not l.strip().startswith("--"))
    for stmt in cleaned.split(";"):
        if stmt.strip():
            execute(conn, stmt)


def fetch(conn, sql, ncols):
    st = conn.createStatement()
    rs = st.executeQuery(sql)
    out = []
    while rs.next():
        out.append(tuple(rs.getString(i + 1) for i in range(ncols)))
    rs.close()
    st.close()
    return out


def jdbc_write(df, table, mode="append"):
    (df.write.format("jdbc").option("url", JDBC_URL).option("dbtable", table)
     .option("user", USER).option("password", PASSWORD).option("driver", "org.postgresql.Driver")
     .option("batchsize", "20000").mode(mode).save())


def jdbc_read_query(spark, query):
    return (spark.read.format("jdbc").option("url", JDBC_URL).option("query", query)
            .option("user", USER).option("password", PASSWORD).option("driver", "org.postgresql.Driver").load())
