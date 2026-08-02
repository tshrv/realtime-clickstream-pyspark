from contextlib import contextmanager

from loguru import logger
from pyspark.sql import SparkSession


@contextmanager
def get_spark_session():
    spark = SparkSession.builder.appName('ClickstreamAnalytics').config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9",
    ).getOrCreate()
    logger.info(f"Running spark version: {spark.version}")
    yield spark
    spark.stop()


def main():
    logger.info("Hello from realtime-clickstream-pyspark!")
    with get_spark_session() as spark:
        logger.info("Spark session created successfully.")
        # Add your Spark processing logic here



if __name__ == "__main__":
    main()
