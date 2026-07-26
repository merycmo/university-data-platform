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

def task_build_faculty():
    import subprocess
    subprocess.run([
        "/opt/spark/bin/spark-submit",
        "--master", "spark://spark-master:7077",
        "--packages", "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "/opt/spark-apps/build_faculty.py", "cadi_ayyad", "FSSM"
    ], check=True)

def task_build_courses():
    import subprocess
    subprocess.run([
        "/opt/spark/bin/spark-submit",
        "--master", "spark://spark-master:7077",
        "--packages", "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "/opt/spark-apps/build_courses.py", "cadi_ayyad", "FSSM"
    ], check=True)

def task_index_elasticsearch():
    from elasticsearch import Elasticsearch
    from minio import Minio
    import json

    es = Elasticsearch("http://university_es:9200")
    client = Minio("minio:9000", access_key="admin", secret_key="password123", secure=False)

    objects = client.list_objects("raw-json", prefix="university=cadi_ayyad/faculty=FSSM/type=authors/", recursive=True)
    
    for obj in objects:
        if obj.object_name.endswith(".meta.json"):
            continue
        try:
            response = client.get_object("raw-json", obj.object_name)
            data = json.loads(response.read().decode("utf-8"))
            es.index(index="university_fssm", body={
                "university": "cadi_ayyad",
                "faculty": "FSSM",
                "type": "author",
                "data": data
            })
        except Exception as e:
            print(f"Erreur indexation : {e}")

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

    build_faculty = PythonOperator(
        task_id         = "build_faculty_profiles",
        python_callable = task_build_faculty
    )

    build_courses = PythonOperator(
        task_id         = "build_course_catalog",
        python_callable = task_build_courses
    )

    index_es = PythonOperator(
        task_id         = "index_elasticsearch",
        python_callable = task_index_elasticsearch
    )

    scraping >> ingest_api >> ingest_file >> build_faculty >> build_courses >> index_es