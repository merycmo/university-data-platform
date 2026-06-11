# dags/pipeline_fsac.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.append('/opt/airflow')

from scraper.university_scraper import scrape_university
from ingestion.ingest_api import fetch_openalex

# ─────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────
default_args = {
    "owner"           : "airflow",
    "retries"         : 3,
    "retry_delay"     : timedelta(minutes=5),
    "start_date"      : datetime(2026, 1, 1),
    "email_on_failure": False
}

# ─────────────────────────────────────────
# DAG
# ─────────────────────────────────────────
with DAG(
    dag_id            = "pipeline_fsac",
    default_args      = default_args,
    schedule_interval = "@daily",
    catchup           = False,
    description       = "Pipeline complet FSAC — Université Hassan II",
    tags              = ["hassan2", "fsac"]
) as dag:

    # ── TASK 1 : Scraping Web ──────────────
    task_scrape_web = PythonOperator(
        task_id         = "scrape_web_fsac",
        python_callable = scrape_university,
        op_kwargs       = {
            "start_url"  : "https://fsac.univh2c.ma/",
            "university" : "hassan2",
            "faculty"    : "FSAC",
            "max_depth"  : 3
        }
    )

    # ── TASK 2 : API OpenAlex ──────────────
    task_fetch_api = PythonOperator(
        task_id         = "fetch_api_fsac",
        python_callable = fetch_openalex,
        op_kwargs       = {
            "university_name" : "Hassan II",
            "faculty_name"    : "FSAC"
        }
    )

    # ── TASK 3 : Spark Transform ───────────
    def run_spark_transform(university, faculty):
        from spark.build_faculty import build_faculty_profiles
        from spark.build_courses import build_course_catalog
        build_faculty_profiles(university=university, faculty=faculty)
        build_course_catalog(university=university, faculty=faculty)

    task_spark = PythonOperator(
        task_id         = "spark_transform_fsac",
        python_callable = run_spark_transform,
        op_kwargs       = {
            "university" : "hassan2",
            "faculty"    : "FSAC"
        }
    )

    # ── TASK 4 : Hudi ─────────────────────
    def run_write_hudi(university, faculty):
        from hudi_hive.create_tables import write_to_hudi
        write_to_hudi(university=university, faculty=faculty)

    task_hudi = PythonOperator(
        task_id         = "write_hudi_fsac",
        python_callable = run_write_hudi,
        op_kwargs       = {
            "university" : "hassan2",
            "faculty"    : "FSAC"
        }
    )

    # ── TASK 5 : Elasticsearch ─────────────
    def run_index_es(university, faculty):
        from search.index_es import index_to_elasticsearch
        index_to_elasticsearch(university=university, faculty=faculty)

    task_es = PythonOperator(
        task_id         = "index_es_fsac",
        python_callable = run_index_es,
        op_kwargs       = {
            "university" : "hassan2",
            "faculty"    : "FSAC"
        }
    )

    # ─────────────────────────────────────────
    # ORDRE D'EXECUTION
    # ─────────────────────────────────────────
    [task_scrape_web, task_fetch_api] >> task_spark >> task_hudi >> task_es