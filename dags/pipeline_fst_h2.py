from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

UNIVERSITY = "hassan2"   # alias reconnu par UNIVERSITY_ALIASES dans build_courses.py / build_faculty.py
FACULTY = "FST"

SPARK_MASTER_CONTAINER = "university_spark_master"

HUDI_PACKAGES_BUILD = (
    "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)

HUDI_PACKAGES_INDEX = (
    "org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.0,"
    "org.elasticsearch:elasticsearch-spark-30_2.12:8.12.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)

S3A_CONFS = (
    "--conf spark.hadoop.fs.s3a.access.key=admin "
    "--conf spark.hadoop.fs.s3a.secret.key=password123 "
    "--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
    "--conf spark.hadoop.fs.s3a.path.style.access=true "
    "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
    "--conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider "
    "--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false"
)

default_args = {
    "owner": "university-data-platform",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=10),
    "retry_exponential_backoff": False,
}

# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id="pipeline_fst_h2",
    description="Pipeline quotidien Université Hassan II / Faculté FST",
    default_args=default_args,
    schedule_interval="0 2 * * *",   # chaque jour à 2h du matin, comme run_daily.sh
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["university", "hassan2", "fst"],
) as dag:

    # --- Étape 1 : Ingestion (les 3 sources), exécutée dans spark-master
    #     via docker exec, comme tu le fais déjà manuellement ---
    ingest_api = BashOperator(
        task_id="ingest_api",
        bash_command=(
            f"docker exec {SPARK_MASTER_CONTAINER} python3 /opt/ingestion/ingest_api.py "
            f"--university {UNIVERSITY} --faculty {FACULTY}"
        ),
    )

    ingest_web = BashOperator(
        task_id="ingest_web",
        bash_command=(
            f"docker exec {SPARK_MASTER_CONTAINER} python3 /opt/ingestion/ingest_web.py "
            f"--university {UNIVERSITY} --faculty {FACULTY}"
        ),
    )

    ingest_file = BashOperator(
        task_id="ingest_file",
        bash_command=(
            f"docker exec {SPARK_MASTER_CONTAINER} python3 /opt/ingestion/ingest_file.py "
            f"--university {UNIVERSITY} --faculty {FACULTY}"
        ),
    )

    # --- Étape 2 : Transformation Spark -> Hudi + Hive ---
    build_courses = BashOperator(
        task_id="build_courses",
        bash_command=(
            f"docker exec {SPARK_MASTER_CONTAINER} /opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "--conf spark.jars.ivy=/tmp/ivy2 "
            f"{S3A_CONFS} "
            f"--packages {HUDI_PACKAGES_BUILD} "
            "/opt/spark-apps/build_courses.py"
        ),
    )

    build_faculty = BashOperator(
        task_id="build_faculty",
        bash_command=(
            f"docker exec {SPARK_MASTER_CONTAINER} /opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "--conf spark.jars.ivy=/tmp/ivy2 "
            f"{S3A_CONFS} "
            f"--packages {HUDI_PACKAGES_BUILD} "
            "/opt/spark-apps/build_faculty.py"
        ),
    )

    # --- Étape 3 : Indexation Elasticsearch ---
    # ⚠️ CORRIGÉ : index_es.py vit dans search/, monté sur /opt/search-apps
    # (pas /opt/spark-apps, qui ne contient que build_courses.py/build_faculty.py)
    index_es = BashOperator(
        task_id="index_es",
        bash_command=(
            f"docker exec {SPARK_MASTER_CONTAINER} /opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "--conf spark.jars.ivy=/tmp/ivy2 "
            "--conf spark.es.nodes=elasticsearch "
            "--conf spark.es.port=9200 "
            "--conf spark.es.nodes.wan.only=true "
            f"{S3A_CONFS} "
            f"--packages {HUDI_PACKAGES_INDEX} "
            "/opt/search-apps/index_es.py"
        ),
    )

    # --- Ordre d'exécution : les 3 ingestions en parallèle, puis la chaîne Spark ---
    [ingest_api, ingest_web, ingest_file] >> build_courses >> build_faculty >> index_es
