import sys
import logging
import os
from datetime import datetime, timedelta

from airflow.sdk import dag, task
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from includes.api.github_client import get_github_repos

logger = logging.getLogger(__name__)

def on_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis):
    for ti in blocking_tis:
        logger.warning("SLA missed: dag_id=%s task_id=%s execution_date=%s", ti.dag_id, ti.task_id, ti.execution_date)

@dag(
    dag_id="orchestration",
    schedule=None,
    start_date=datetime(2026, 3, 2),
    catchup=False,
    sla_miss_callback=on_sla_miss,
)
def orchestration():
    logger.info("Orchestration from DAG")

    @task(
        retries=3,
        retry_delay=timedelta(minutes=2),
        retry_exponential_backoff=False,
        max_retry_delay=timedelta(minutes=10),
        sla=timedelta(minutes=10),
    )
    def fetch_and_upload_to_s3():
        from airflow.sdk import get_current_context

        context = get_current_context()
        logical_date = context.get("logical_date")
        logger.info(f"Logical date: {logical_date}")

        s3_key = get_github_repos(logical_date)
        logger.info(f"S3 key: {s3_key}")

    fetch_and_upload_to_s3()

orchestration()
