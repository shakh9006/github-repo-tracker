import logging
import sys

from datetime import datetime
from airflow.sdk import dag, task
from airflow import settings

sys.path.append('/opt/airflow/plugins')

from dbt_operator import DbtCoreOperator

DBT_PROJECT_PATH = f"{settings.AIRFLOW_HOME}/github_repo_tracker"

logger = logging.getLogger(__name__)

@dag(
    dag_id="hello",
    schedule=None,
    start_date=datetime(2026, 2, 24),
    catchup=False,
)
def hello_world():
    logger.info("Hello, World from dag")

    @task
    def start():
        logger.info("Start from task")
    
    dbt_operator = DbtCoreOperator(
        task_id="dbt_run",
        dbt_project_dir=DBT_PROJECT_PATH,
        dbt_profiles_dir=DBT_PROJECT_PATH,
        dbt_command="run",
    )

    @task
    def end():
        logger.info("End from task")
    
    start() >> dbt_operator >> end()

hello_world()