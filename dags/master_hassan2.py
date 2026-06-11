# dags/master_hassan2.py

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

# ─────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────
default_args = {
    "owner"          : "airflow",
    "retries"        : 2,
    "retry_delay"    : timedelta(minutes=5),
    "start_date"     : datetime(2026, 1, 1),
    "email_on_failure": False
}

# ─────────────────────────────────────────
# DAG MASTER HASSAN II
# ─────────────────────────────────────────
with DAG(
    dag_id            = "master_hassan2",
    default_args      = default_args,
    schedule_interval = "@daily",
    catchup           = False,
    description       = "DAG Master — Lance les 3 facultés Hassan II en parallèle",
    tags              = ["master", "hassan2"]
) as dag:

    # ── Lancer FSAC ───────────────────────
    trigger_fsac = TriggerDagRunOperator(
        task_id             = "trigger_pipeline_fsac",
        trigger_dag_id      = "pipeline_fsac",
        wait_for_completion = True,
        poke_interval       = 60
    )

    # ── Lancer FSBM ───────────────────────
    trigger_fsbm = TriggerDagRunOperator(
        task_id             = "trigger_pipeline_fsbm",
        trigger_dag_id      = "pipeline_fsbm",
        wait_for_completion = True,
        poke_interval       = 60
    )

    # ── Lancer FST ────────────────────────
    trigger_fst = TriggerDagRunOperator(
        task_id             = "trigger_pipeline_fst_h2",
        trigger_dag_id      = "pipeline_fst_h2",
        wait_for_completion = True,
        poke_interval       = 60
    )

    # ── Les 3 facultés EN PARALLÈLE ───────
    [trigger_fsac, trigger_fsbm, trigger_fst]