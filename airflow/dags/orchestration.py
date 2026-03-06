import sys
import logging
import os
from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk.definitions.deadline import AsyncCallback, DeadlineAlert, DeadlineReference

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from includes.api.github_client import get_github_repos

logger = logging.getLogger(__name__)

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
WAREHOUSE_PATH = os.getenv("S3_BUCKET", "s3://github-repo-tracker")
NESSIE_URI = os.getenv("NESSIE_URI", "http://nessie:19120/api/v1")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")

async def on_sla_miss(text):
    logger.warning(text)

@dag(
    dag_id="orchestration",
    schedule=timedelta(minutes=30),
    start_date=datetime(2026, 3, 2),
    catchup=False,
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_LOGICAL_DATE,
        interval=timedelta(minutes=10),
        callback=AsyncCallback(
            on_sla_miss,
            kwargs={"text": "Deadline missed for {{ dag_run.dag_id }}"},
        ),
    ),
)
def orchestration():
    logger.info("Orchestration from DAG")

    @task(
        retries=3,
        retry_delay=timedelta(minutes=2),
        retry_exponential_backoff=False,
        max_retry_delay=timedelta(minutes=10),
    )
    def fetch_and_upload_to_s3():
        from airflow.sdk import get_current_context

        context = get_current_context()
        logical_date = context.get("logical_date")
        logger.info(f"Logical date: {logical_date}")

        s3_key = get_github_repos(logical_date)
        context["ti"].xcom_push(key="s3_json_path", value=s3_key)

    loading_to_iceberg = SparkSubmitOperator(
        task_id="load_to_iceberg",
        conn_id="spark_default",
        application="/opt/airflow/includes/spark/load_to_s3.py",
        application_args=["{{ ti.xcom_pull(task_ids='fetch_and_upload_to_s3', key='s3_json_path') }}"],
        verbose=True,
        jars=(
            "file:///opt/spark/jars/custom/iceberg-spark-runtime-3.5_2.12-1.9.2.jar,"
            "file:///opt/spark/jars/custom/iceberg-aws-bundle-1.9.2.jar,"
            "file:///opt/spark/jars/custom/nessie-spark-extensions-3.5_2.12-0.102.5.jar,"
            "file:///opt/spark/jars/custom/hadoop-aws-3.3.4.jar,"
            "file:///opt/spark/jars/custom/aws-java-sdk-bundle-1.12.262.jar"
        ),
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
            "spark.sql.defaultCatalog": "nessie",
            "spark.sql.catalog.nessie": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.nessie.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
            "spark.sql.catalog.nessie.uri": NESSIE_URI,
            "spark.sql.catalog.nessie.ref": "main",
            "spark.sql.catalog.nessie.warehouse": WAREHOUSE_PATH,
            "spark.sql.catalog.nessie.authentication.type": "NONE",
            "spark.sql.catalog.nessie.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            "spark.sql.catalog.nessie.s3.endpoint": MINIO_ENDPOINT,
            "spark.sql.catalog.nessie.s3.path-style-access": "true",
            "spark.sql.catalog.nessie.s3.region": AWS_REGION,
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.access.key": AWS_ACCESS_KEY_ID,
            "spark.hadoop.fs.s3a.secret.key": AWS_SECRET_ACCESS_KEY,
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        },
    )

    fetch_and_upload_to_s3() >> loading_to_iceberg

orchestration()
