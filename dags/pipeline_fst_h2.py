# dags/pipeline_fst_h2.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.append('/opt/airflow')

from scraper.university_scraper import scrape_university
from ingestion.ingest_api import fetch_openalex

default_args = {
    "owner"           : "airflow",
    "retries"         : 3,
    "retry_delay"     : timedelta(minutes=5),
    "start_date"      : datetime(2026, 1, 1),
    "email_on_failure": False
}

with DAG(
    dag_id            = "pipeline_fst_h2",
    default_args      = default_args,
    schedule_interval = "@daily",
    catchup           = False,
    description       = "Pipeline FST — Hassan II",
    tags              = ["hassan2", "fst"]
) as dag:

    task_scrape_web = PythonOperator(
        task_id         = "scrape_web_fst",
        python_callable = scrape_university,
        op_kwargs       = {
            "start_url"  : "https://www.fstm.ac.ma/",
            "university" : "hassan2",
            "faculty"    : "FST",
            "max_depth"  : 3
        }
    )

    task_fetch_api = PythonOperator(
        task_id         = "fetch_api_fst",
        python_callable = fetch_openalex,
        op_kwargs       = {
            "university_name" : "Hassan II",
            "faculty_name"    : "FST"
        }
    )

    def run_spark_transform(university, faculty):
        from spark.build_faculty import build_faculty_profiles
        from spark.build_courses import build_course_catalog
        build_faculty_profiles(university=university, faculty=faculty)
        build_course_catalog(university=university, faculty=faculty)

    task_spark = PythonOperator(
        task_id         = "spark_transform_fst",
        python_callable = run_spark_transform,
        op_kwargs       = {
            "university" : "hassan2",
            "faculty"    : "FST"
        }
    )

    def run_write_hudi(university, faculty):
        from hudi_hive.create_tables import write_to_hudi
        write_to_hudi(university=university, faculty=faculty)

    task_hudi = PythonOperator(
        task_id         = "write_hudi_fst",
        python_callable = run_write_hudi,
        op_kwargs       = {
            "university" : "hassan2",
            "faculty"    : "FST"
        }
    )

    def run_index_es(university, faculty):
        from search.index_es import index_to_elasticsearch
        index_to_elasticsearch(university=university, faculty=faculty)

    task_es = PythonOperator(
        task_id         = "index_es_fst",
        python_callable = run_index_es,
        op_kwargs       = {
            "university" : "hassan2",
            "faculty"    : "FST"
        }
    )

    [task_scrape_web, task_fetch_api] >> task_spark >> task_hudi >> task_es