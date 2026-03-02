import os
import requests
import logging
import time
import json
import boto3
from botocore.config import Config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

BACKOFF_SECONDS = [5, 10, 20]
MAX_RETRIES = 3
REQUEST_TIMEOUT = 10

def get_github_repos(logical_date):
    logger.info("Starting Github ingestion...")

    repos = []
    successful_languages = 0

    popular_languages = [
        "python", "javascript", "java", "c", "c++", "c#", "php", "ruby",
        "swift", "kotlin", "go", "rust", "elixir", "erlang", "haskell", "sql",
    ]

    headers = {
        "Authorization": f"Bearer {os.getenv('GITHUB_TOKEN')}",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    for language in popular_languages:
        url = (
            f"https://api.github.com/search/repositories"
            f"?q=language:{language}&sort=stars&order=desc&per_page=10"
        )

        attempt = 0

        while attempt < MAX_RETRIES:
            attempt += 1
            logger.info("Attempt %s/%s for language: %s", attempt, MAX_RETRIES, language)
            
            try:
                response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            except requests.RequestException as e:
                logging.error("Network error for language: %s: %s", language, e)
                time.sleep(BACKOFF_SECONDS[min(attempt - 1, len(BACKOFF_SECONDS) - 1)])
                continue

            if response.status_code == 200:
                logger.info("Success for language: %s, repos: %s", language, len(response.json().get("items", [])))
                data = response.json()
                repos.extend(data.get("items", []))
                successful_languages += 1
                break

            if response.status_code in (403, 429):
                delay = BACKOFF_SECONDS[min(attempt - 1, len(BACKOFF_SECONDS) - 1)]
                logger.warning("Rate limit exceeded for language: %s. Waiting %s seconds.", language, delay)
                time.sleep(delay)
                continue

            logger.info("Unexpected status: ", response.status_code)
            break

        time.sleep(1)

    sla_coverage = successful_languages / len(popular_languages) * 100
    logger.info("SLA coverage: %.2f%%", sla_coverage)

    if sla_coverage < 100:
        logger.warning("SLA violation detected.")

    if len(repos) == 0:
        logger.warning("No repositories found.")

    s3_key = f"github_repos_temp/data_{logical_date.strftime("%Y-%m-%d-%H-%M-%S")}.json"

    body = json.dumps(repos, ensure_ascii=False).encode("utf-8")
    # s3 = boto3.client("s3",
    #     endpoint_url=os.getenv("MINIO_ENDPOINT_URL"),
    #     region_name=os.getenv("AWS_REGION"),
    #     aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    #     aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    #     config=Config(signature_version="s3v4"),
    # )

    s3 = boto3.client("s3",
        endpoint_url="http://minio:9000",
        region_name="us-east-1",
        aws_access_key_id="tAWhZbyU4CzL15TKPJEg",
        aws_secret_access_key="3fnNJ0fFpzpDo4tzmZMM7RiyuiKMI8c9YS4iVZiM",
        config=Config(signature_version="s3v4"),
    )

    s3.put_object(
        Bucket=os.getenv("S3_BUCKET_NAME"),
        Key=s3_key,
        Body=body,
        ContentType="application/json"
    )

    logger.info("Uploaded to S3: %s", os.getenv("S3_BUCKET_NAME"))

    return s3_key