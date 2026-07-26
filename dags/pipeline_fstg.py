"""
DAG University Data Platform — Pipeline complet
=================================================

Orchestre, dans l'ordre :
  1. Scraping web (ingestion/ingest_web.py)                 -> MinIO raw-web-html / raw-documents
  2. Ingestion API OpenAlex (ingestion/ingest_api.py)        -> MinIO raw-json (authors, publications)
  3. Extraction texte PDF/DOCX (ingestion/ingest_file.py)    -> MinIO raw-json (texte extrait)
  4. build_faculty.py  (Spark -> Hudi faculty_profiles), dépend de (2)
  5. build_courses.py  (Spark -> Hudi course_catalog), dépend de (1) + (3)

Les étapes d'ingestion (1, 2, 3) s'exécutent comme du code Python
directement dans le conteneur Airflow (PythonOperator).
Les jobs Spark (4, 5) s'exécutent via spark-submit DANS le conteneur
spark-master déjà démarré, grâce au SDK Docker Python (pas de docker
exec depuis l'hôte nécessaire).
"""

from datetime import datetime, timedelta

import docker
from airflow import DAG
from airflow.operators.python import PythonOperator

# ──────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────

SPARK_MASTER_CONTAINER = "university_spark_master"

# Nom utilisé pour la recherche API OpenAlex (texte libre, peu importe le format)
API_UNIVERSITY_NAME = "Cadi Ayyad"

# Nom utilisé pour les CHEMINS MinIO (scrape_web + ingest_file) — doit être
# EXACTEMENT identique entre ces deux étapes, car ni ingest_web.py ni
# ingest_file.py ne normalisent le nom (contrairement aux jobs Spark qui
# essaient plusieurs variantes automatiquement).
# "cadi_ayyad" est une des variantes déjà testées par build_faculty.py ET
# build_courses.py, donc ce choix garantit que les deux jobs Spark
# retrouveront bien les données.
PATH_UNIVERSITY = "cadi_ayyad"

FACULTY = "FSTG"
START_URL = "https://www.uca.ma/fstg"  # adapte à l'URL réelle de la faculté

SPARK_SUBMIT_BASE = (
    "/opt/spark/bin/spark-submit "
    "--master spark://spark-master:7077 "
    "--conf spark.jars.ivy=/tmp/ivy2 "
    "--conf spark.hadoop.fs.s3a.access.key=admin "
    "--conf spark.hadoop.fs.s3a.secret.key=password123 "
    "--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
    "--conf spark.hadoop.fs.s3a.path.style.access=true "
    "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
    "--conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider "
    "--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false "
    "{script_path}"
)


# ──────────────────────────────────────────────────────────
# Fonctions Python (ingestion)
# ──────────────────────────────────────────────────────────

def task_scrape_web(**context):
    from ingestion.ingest_web import scrape_university

    stats = scrape_university(
        start_url=START_URL,
        university=PATH_UNIVERSITY,
        faculty=FACULTY,
        max_depth=2,
    )
    print(f"Scraping termine : {stats}")
    if stats.get("errors", 0) > 0 and stats.get("html", 0) == 0 and stats.get("pdf", 0) == 0:
        raise RuntimeError("Scraping n'a rien collecte et a rencontre des erreurs.")


def task_ingest_api(**context):
    from ingestion.ingest_api import ingest_faculty

    ingest_faculty(university_name=API_UNIVERSITY_NAME, faculty_name=FACULTY)


def task_ingest_file(**context):
    from ingestion.ingest_file import run_file_ingestion

    # PATH_UNIVERSITY est déjà au format exact des chemins MinIO
    # (identique à celui utilisé par scrape_web)
    stats = run_file_ingestion(university=PATH_UNIVERSITY, faculty=FACULTY)
    print(f"Extraction terminee : {stats}")


# ──────────────────────────────────────────────────────────
# Fonction generique d'execution Spark via Docker SDK
# ──────────────────────────────────────────────────────────

def run_spark_script(script_path: str):
    """Execute spark-submit sur un script donne, dans le conteneur spark-master."""
    client = docker.from_env()
    container = client.containers.get(SPARK_MASTER_CONTAINER)

    cmd = SPARK_SUBMIT_BASE.format(script_path=script_path)
    print(f"Executing in container '{SPARK_MASTER_CONTAINER}':\n{cmd}")

    exit_code, output = container.exec_run(
        cmd=["/bin/bash", "-c", cmd],
        stdout=True,
        stderr=True,
        demux=False,
    )

    log_output = output.decode("utf-8", errors="replace") if output else ""
    print(log_output)

    if exit_code != 0:
        raise RuntimeError(
            f"spark-submit a echoue (code {exit_code}) pour {script_path}. "
            f"Voir les logs ci-dessus."
        )
    print(f"Job Spark '{script_path}' termine avec succes.")


def task_build_faculty_profiles(**context):
    run_spark_script("/opt/spark-apps/build_faculty.py")


def task_build_course_catalog(**context):
    run_spark_script("/opt/spark-apps/build_courses.py")


# ──────────────────────────────────────────────────────────
# Definition du DAG
# ──────────────────────────────────────────────────────────

default_args = {
    "owner": "university-data-platform",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
}

with DAG(
    dag_id="university_pipeline",
    description="Pipeline complet: scraping web + API + fichiers -> Spark -> Hudi/Hive",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["university", "ingestion", "spark", "hudi"],
) as dag:

    scrape_web = PythonOperator(
        task_id="scrape_web",
        python_callable=task_scrape_web,
    )

    ingest_api = PythonOperator(
        task_id="ingest_api",
        python_callable=task_ingest_api,
    )

    ingest_file = PythonOperator(
        task_id="ingest_file",
        python_callable=task_ingest_file,
    )

    build_faculty_profiles = PythonOperator(
        task_id="build_faculty_profiles",
        python_callable=task_build_faculty_profiles,
    )

    build_course_catalog = PythonOperator(
        task_id="build_course_catalog",
        python_callable=task_build_course_catalog,
    )

    # Dependances :
    # - le scraping web doit finir avant l'extraction de fichiers (les PDFs
    #   scrapes doivent etre presents dans raw-documents avant extraction)
    # - build_course_catalog depend du texte extrait (ingest_file)
    # - build_faculty_profiles depend des donnees API (ingest_api)
    scrape_web >> ingest_file >> build_course_catalog
    ingest_api >> build_faculty_profiles
