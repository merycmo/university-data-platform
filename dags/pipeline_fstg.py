# dags/pipeline_fstg.py

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scraper.university_scraper import scrape_university

# -----------------------------------------
# Configuration
# -----------------------------------------
default_args = {
    "owner"            : "airflow",
    "retries"          : 2,
    "retry_delay"      : timedelta(minutes=5),
    "start_date"       : datetime(2026, 1, 1),
    "email_on_failure" : False
}

# -----------------------------------------
# Fonction de scraping FSTG
# -----------------------------------------
def scrape_fstg():
    stats = scrape_university(
        start_url  = "https://www.fstg-marrakech.ac.ma/",
        university = "cadi_ayyad",
        faculty    = "FSTG",
        max_depth  = 3
    )
    print(f"HTML      : {stats['html']}")
    print(f"PDFs      : {stats['pdf']}")
    print(f"Images    : {stats['image']}")
    return stats

# -----------------------------------------
# DAG FSTG Marrakech
# -----------------------------------------
with DAG(
    dag_id            = "pipeline_fstg",
    default_args      = default_args,
    schedule_interval = "@daily",
    catchup           = False,
    description       = "Pipeline scraping FSTG Marrakech",
    tags              = ["fstg", "cadi_ayyad"]
) as dag:

    scrape_task = PythonOperator(
        task_id         = "scrape_fstg_marrakech",
        python_callable = scrape_fstg
    )

    scrape_task