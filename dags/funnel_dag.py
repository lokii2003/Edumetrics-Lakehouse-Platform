"""
============================================================================
FILE: dags/funnel_dag.py — Apache Airflow DAG
============================================================================

OBJECTIVE:
    Orchestrate the full EduMetrics pipeline on a daily schedule.
    data_generator → bronze_pipeline → silver_gold_pipeline

KEY CONCEPTS:
    - DAG (Directed Acyclic Graph): defines task execution order
    - PythonOperator: runs Python functions as Airflow tasks
    - Retries: automatically re-run failed tasks
    - Catchup=False: don't backfill historical runs on first deploy

INTERVIEW QUESTIONS:
    1. What is an Airflow DAG?
       → A collection of tasks with defined dependencies and schedule.
    2. What does catchup=False mean?
       → Don't run DAG for past dates when deployed. Only run future.
    3. Why use PythonOperator instead of BashOperator?
       → Gives us Python error handling, logging, and return values.
    4. What happens if a task fails?
       → Airflow retries it (configured: 2 retries, 5 min delay).
============================================================================
"""

import sys
import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Add project root to path so we can import our pipeline modules
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────
# TASK FUNCTIONS — Each one wraps a pipeline script
# ──────────────────────────────────────────────────────────────────────────

def run_data_generator(**kwargs):
    """Task 1: Generate simulated user activity data."""
    from data_generator import main as generate
    logger.info("Starting data generation...")
    generate()
    logger.info("Data generation complete.")


def run_bronze_pipeline(**kwargs):
    """Task 2: Ingest raw data → Bronze Delta table."""
    from pipeline_bronze import run_bronze_pipeline
    logger.info("Starting Bronze pipeline...")
    result = run_bronze_pipeline()
    logger.info("Bronze pipeline complete: %s", result)


def run_silver_gold_pipeline(**kwargs):
    """Task 3: Transform Bronze → Silver → Gold."""
    from pipeline_silver_gold import run_pipeline
    logger.info("Starting Silver/Gold pipeline...")
    run_pipeline()
    logger.info("Silver/Gold pipeline complete.")


# ──────────────────────────────────────────────────────────────────────────
# DAG DEFINITION
# ──────────────────────────────────────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,                         # Retry failed tasks twice
    "retry_delay": timedelta(minutes=5),  # Wait 5 min between retries
    "email_on_failure": False,
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="edumetrics_funnel_pipeline",
    default_args=default_args,
    description="E-Learning Funnel Analytics — Daily Pipeline",
    schedule_interval="0 2 * * *",  # Run daily at 2:00 AM UTC
    start_date=datetime(2026, 6, 1),
    catchup=False,                  # Don't backfill past dates
    max_active_runs=1,              # Only one run at a time
    tags=["edumetrics", "data-engineering", "delta-lake"],
) as dag:

    # Define tasks
    task_generate = PythonOperator(
        task_id="generate_data",
        python_callable=run_data_generator,
    )

    task_bronze = PythonOperator(
        task_id="bronze_pipeline",
        python_callable=run_bronze_pipeline,
    )

    task_silver_gold = PythonOperator(
        task_id="silver_gold_pipeline",
        python_callable=run_silver_gold_pipeline,
    )

    # ── Task Dependencies ──
    # generate_data → bronze_pipeline → silver_gold_pipeline
    task_generate >> task_bronze >> task_silver_gold

"""
SAMPLE DAG VISUALIZATION:

    [generate_data] → [bronze_pipeline] → [silver_gold_pipeline]

AIRFLOW UI:
    Schedule: Daily at 02:00 UTC
    Tags: edumetrics, data-engineering, delta-lake
"""
