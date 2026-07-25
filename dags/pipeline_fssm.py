# dags/pipeline_fssm.py
# Membre 4 - Hafsa Masrour
# Faculté des Sciences Semlalia - Marrakech (FSSM)

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

default_args = {
    "owner"           : "hafsa",
    "retries"         : 1,
    "retry_delay"     : timedelta(minutes=5),
    "start_date"      : datetime(2026, 1, 1),
    "email_on_failure": False
}

def task_scraping():
    from scraper.university_scraper import scrape_university
    return scrape_university(
        start_url  = "https://www.uca.ma/fssm/fr",
        university = "cadi_ayyad",
        faculty    = "FSSM",
        max_depth  = 3
    )

def task_ingest_api():
    from ingestion.ingest_api import ingest_faculty
    ingest_faculty("Cadi Ayyad", "FSSM")

def task_ingest_file():
    from ingestion.ingest_file import run_file_ingestion
    run_file_ingestion(
        university = "cadi_ayyad",
        faculty    = "FSSM"
    )

with DAG(
    dag_id            = "pipeline_fssm",
    default_args      = default_args,
    schedule_interval = "@daily",
    catchup           = False,
    description       = "Pipeline FSSM - Cadi Ayyad",
    tags              = ["fssm", "cadi_ayyad"]
) as dag:

    scraping = PythonOperator(
        task_id         = "scraping_web",
        python_callable = task_scraping
    )

    ingest_api = PythonOperator(
        task_id         = "ingestion_api",
        python_callable = task_ingest_api
    )

    ingest_file = PythonOperator(
        task_id         = "ingestion_fichiers",
        python_callable = task_ingest_file
    )

    scraping >> ingest_api >> ingest_file