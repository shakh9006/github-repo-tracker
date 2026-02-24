import logging
from datetime import datetime

from airflow.sdk import dag, task

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
    
    @task
    def print_hello():
        logger.info("Hello, World from task")

    @task
    def end():
        logger.info("End from task")
    
    start() >> print_hello() >> end()

hello_world()