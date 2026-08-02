from loguru import logger
from pyspark.sql import SparkSession


def get_spark_session():
    spark = SparkSession.builder.appName('ClickstreamAnalytics').config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9",
    ).getOrCreate()
    logger.info(f"Running spark version: {spark.version}")
    return spark


def main():
    logger.info("Hello from realtime-clickstream-pyspark!")
    spark = get_spark_session()



if __name__ == "__main__":
    main()
