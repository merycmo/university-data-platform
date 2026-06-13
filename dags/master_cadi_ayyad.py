# dags/master_cadi_ayyad.py

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

# ─────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────
default_args = {
    "owner"           : "airflow",
    "retries"         : 2,
    "retry_delay"     : timedelta(minutes=5),
    "start_date"      : datetime(2026, 1, 1),
    "email_on_failure": False
}

# ─────────────────────────────────────────
# DAG MASTER CADI AYYAD
# ─────────────────────────────────────────
with DAG(
    dag_id            = "master_cadi_ayyad",
    default_args      = default_args,
    schedule_interval = "@daily",
    catchup           = False,
    description       = "DAG Master — Lance les 3 facultés Cadi Ayyad en parallèle",
    tags              = ["master", "cadi_ayyad"]
) as dag:

    # ── Lancer FSSM ───────────────────────
    trigger_fssm = TriggerDagRunOperator(
        task_id             = "trigger_pipeline_fssm",
        trigger_dag_id      = "pipeline_fssm",
        wait_for_completion = True,
        poke_interval       = 60
    )

    # ── Lancer FST Marrakech ──────────────
    trigger_fst = TriggerDagRunOperator(
        task_id             = "trigger_pipeline_fst_uca",
        trigger_dag_id      = "pipeline_fst_uca",
        wait_for_completion = True,
        poke_interval       = 60
    )

    # ── Lancer FSTG ───────────────────────
    trigger_fstg = TriggerDagRunOperator(
        task_id             = "trigger_pipeline_fstg",
        trigger_dag_id      = "pipeline_fstg",
        wait_for_completion = True,
        poke_interval       = 60
    )

    # ── Les 3 facultés EN PARALLÈLE ───────
    [trigger_fssm, trigger_fst, trigger_fstg]