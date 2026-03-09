import os
import logging
import sys
import re

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

S3_JSON_PATH = sys.argv[1] if len(sys.argv) > 1 else os.getenv("S3_JSON_PATH")
if not S3_JSON_PATH:
    raise ValueError("S3 JSON path required: pass as argument or set S3_JSON_PATH")

TABLE_NAME = os.getenv("RAW_ICEBERG_TABLE", "nessie.raw.github_repositories_raw")

def extract_batch_id_from_path(path):
    m = re.search(r"data_(\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2})\.json$", path)
    if m:
        return m.group(1)

    return path.rsplit("/", 1)[-1].replace(".json", "")

def ensure_namespace_and_table(spark, table_name):
    parts = table_name.split(".")
    if len(parts) != 3:
        raise ValueError(f"Invalid table name: {table_name}")

    catalog, namespace, _ = parts

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            _source_file STRING,
            _batch_id STRING,
            _ingested_at TIMESTAMP,
            raw_payload STRING
        )
        USING iceberg
        PARTITIONED BY (_batch_id)
        """
    )

def main():
    spark = (
        SparkSession.builder
        .appName("Load to S3")
        .getOrCreate()
    )

    try:
        logger.info(f"Reading JSON from: {S3_JSON_PATH}")

        df = (
            spark.read
            .option("multiLine", True)
            .json(S3_JSON_PATH)
        )

        if df.rdd.isEmpty():
            logger.warning(f"No data found in {S3_JSON_PATH}")
            return

        batch_id = extract_batch_id_from_path(S3_JSON_PATH)

        df_raw = (
            df
            .withColumn("_source_file", F.lit(S3_JSON_PATH))
            .withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("raw_payload", F.to_json(F.struct(*[F.col(c) for c in df.columns])))
            .select("_source_file", "_batch_id", "_ingested_at", "raw_payload")
        )

        ensure_namespace_and_table(spark, TABLE_NAME)

        escaped_path = S3_JSON_PATH.replace("'", "''")
        spark.sql(f"DELETE FROM {TABLE_NAME} WHERE _source_file = '{escaped_path}'")

        df_raw.writeTo(TABLE_NAME).append()

        row_count = df_raw.count()

        logger.info("Written %s rows to %s for source=%s", row_count, TABLE_NAME, S3_JSON_PATH)

        
    except Exception as e:
        logger.error(f"Error occurred while rewriting table to S3: {e}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    main()