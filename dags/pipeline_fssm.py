# dags/pipeline_fssm.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.insert(0, '/opt/airflow')
sys.path.insert(0, '/opt/airflow/dags')
sys.path.insert(0, '/')
from university_scraper import scrape_university
from ingestion.ingest_api import fetch_openalex

default_args = {
    "owner"           : "airflow",
    "retries"         : 3,
    "retry_delay"     : timedelta(minutes=5),
    "start_date"      : datetime(2026, 1, 1),
    "email_on_failure": False
}

with DAG(
    dag_id            = "pipeline_fssm",
    default_args      = default_args,
    schedule_interval = "@daily",
    catchup           = False,
    description       = "Pipeline complet FSSM — Université Cadi Ayyad",
    tags              = ["cadi_ayyad", "fssm"]
) as dag:

    task_scrape_web = PythonOperator(
        task_id         = "scrape_web_fssm",
        python_callable = scrape_university,
        op_kwargs       = {
            "start_url"  : "https://www.uca.ma/fssm/fr",
            "university" : "cadi_ayyad",
            "faculty"    : "FSSM",
            "max_depth"  : 3
        }
    )

    task_fetch_api = PythonOperator(
        task_id         = "fetch_api_fssm",
        python_callable = fetch_openalex,
        op_kwargs       = {
            "university_name" : "Cadi Ayyad",
            "faculty_name"    : "FSSM"
        }
    )

    def run_spark_transform(university, faculty):
        from spark.build_faculty import build_faculty_profiles
        from spark.build_courses import build_course_catalog
        build_faculty_profiles(university=university, faculty=faculty)
        build_course_catalog(university=university, faculty=faculty)

    task_spark = PythonOperator(
        task_id         = "spark_transform_fssm",
        python_callable = run_spark_transform,
        op_kwargs       = {
            "university" : "cadi_ayyad",
            "faculty"    : "FSSM"
        }
    )

    def run_write_hudi(university, faculty):
        from hudi_hive.create_tables import write_to_hudi
        write_to_hudi(university=university, faculty=faculty)

    task_hudi = PythonOperator(
        task_id         = "write_hudi_fssm",
        python_callable = run_write_hudi,
        op_kwargs       = {
            "university" : "cadi_ayyad",
            "faculty"    : "FSSM"
        }
    )

    def run_index_es(university, faculty):
        from search.index_es import index_to_elasticsearch
        index_to_elasticsearch(university=university, faculty=faculty)

    task_es = PythonOperator(
        task_id         = "index_es_fssm",
        python_callable = run_index_es,
        op_kwargs       = {
            "university" : "cadi_ayyad",
            "faculty"    : "FSSM"
        }
    )

    [task_scrape_web, task_fetch_api] >> task_spark >> task_hudi >> task_es