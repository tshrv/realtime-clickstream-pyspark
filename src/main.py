from contextlib import contextmanager

from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window
from pyspark.sql.types import StringType, StructField, StructType, TimestampType


@contextmanager
def get_spark_session():
    spark = (
        SparkSession.builder.appName("ClickstreamAnalytics")
        .config("spark.sql.streaming.schemaInference", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"Running spark version: {spark.version}")
    yield spark
    spark.stop()


def process_streaming_data(spark: SparkSession):
    # Define schema matching producer's JSON events
    schema = StructType(
        [
            StructField("event_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("page", StringType(), True),
        ]
    )
    # Read from Kafka
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:29092")
        .option("subscribe", "clickstream_events")
        .option("startingOffsets", "latest") # only reads events produced after it starts
        .option("maxOffsetsPerTrigger", 1000) # caps how many messages enter each micro-batch
        .load()
    )

    # Parse JSON and cast timestamp
    events = (
        raw_stream.selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), schema).alias("data"))
        .select("data.*")
        .withColumn("timestamp", col("timestamp").cast(TimestampType()))
    )

    # The schema tells Spark how to parse the JSON strings your producer sends.
    # Each Kafka message arrives as a binary value column, so you cast it to a string, then parse it with from_json.
    # The maxOffsetsPerTrigger option caps how many messages enter each micro-batch.
    # This is your primary backpressure knob for Kafka sources.

    # query = (
    #     events.writeStream.outputMode("append")
    #     .format("console")
    #     .option("truncate", "false")
    #     .start()
    # )
    # query.awaitTermination()

    # Spark needs to know how late data can arrive.
    # Your producer sends events up to 90 seconds late,
    # so a 2-minute watermark covers those late arrivals comfortably.
    # Replace it with a watermark, deduplication, and windowed aggregation
    deduplicated = events.withWatermark("timestamp", "2 minutes").dropDuplicatesWithinWatermark(["event_id"])
    # any event arriving more than 2 minutes behind the latest observed timestamp can be dropped.
    # Once the watermark passes a window's end,
    # Spark knows no more data will arrive for that window and emits the final count.
    # Without a watermark, deduplication state would grow forever.

    # Windowed aggregation: count events per type per minute
    windowed_counts = deduplicated.groupBy(
        window(col("timestamp"), "1 minute"),
        col("event_type"),
    ).count()

    # Write aggregated counts to console
    query = (
        windowed_counts.writeStream.outputMode("append")
        # events.writeStream.outputMode("append")
        # pyspark.errors.exceptions.captured.AnalysisException: Append output mode not supported when there are streaming aggregations on streaming DataFrames/DataSets without watermark
        .format("console")
        .option("truncate", "false")
        .start()
    )

    query.awaitTermination()


def main():
    logger.info("Hello from realtime-clickstream-pyspark!")
    with get_spark_session() as spark:
        logger.info("Spark session created successfully.")
        # Add your Spark processing logic here
        logger.info("Processing started")
        process_streaming_data(spark)
        logger.info("Processing stopped")


if __name__ == "__main__":
    main()
