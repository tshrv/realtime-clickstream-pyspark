from contextlib import contextmanager

from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, struct, to_json, window
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

# foreachBatch handler: write each micro-batch to the kafka analytics topic

def write_to_sinks(batch_df, batch_id):
    """
    The write_to_sinks function receives two arguments every micro-batch: the batch DataFrame and a unique batch ID.
    - batch_df.isEmpty() skips empty batches that occur before any windows finalize.
    - batch_df.persist() caches the batch in memory so Spark does not recompute it if you add more sinks later.
    - to_json(struct(...)) serializes each row into a JSON string under a value column, which is what Kafka expects.
    - kafka_output.write.format("kafka") uses a standard batch write to push the data to the clickstream-analytics topic.
    - `batch_df.unpersist() frees the cached memory once all writes are done.
    """
    print(f"write_to_sinks: Processing batch {batch_id} with {batch_df.count()} rows")
    print(batch_df.show(truncate=False))
    # skip empty batches, no finalized windows yet
    if batch_df.isEmpty():
        return
    # cache the batch sp we don't recompute for each sink
    batch_df.persist()
    # format windowed counts as JSON for the kafka value column
    kafka_output = batch_df.select(
        to_json(
            struct(
                col("window").getField("start").alias("window_start"),
                col("window").getField("end").alias("window_end"),
                col("event_type"),
                col("count"),
            )
        ).alias("value")
    )
    # write this batch to the downstream analytics topic
    (
        kafka_output.write.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:29092")
        .option("topic", "clickstream_analytics")
        .save()
    )
    batch_df.unpersist()


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
    # query = (
    #     windowed_counts.writeStream.outputMode("append")
    #     # events.writeStream.outputMode("append")
    #     # pyspark.errors.exceptions.captured.AnalysisException: Append output mode not supported when there are streaming aggregations on streaming DataFrames/DataSets without watermark
    #     .format("console")
    #     .option("truncate", "false")
    #     .start()
    # )

    # Start the streaming query with foreachBatch, checkpoint, and trigger
    query = (
        windowed_counts.writeStream.outputMode("append")
        .foreachBatch(write_to_sinks)
        .option("checkpointLocation", "/checkpoints")
        # if mounting as volume, chmod 777 so that each spark container can write to it.
        # other options is to use s3 path like s3a://mybucket/checkpoints, that requires AWS credentials in the spark config
        # Spark stores Kafka offsets in its own checkpoint directory, not in Kafka consumer groups. The startingOffsets option is ignored after the first successful checkpoint.
        # On restart, Spark reads the offsets and commits files to determine the last successfully processed micro-batch. It then resumes from the next offset, providing exactly-once processing semantics for the pipeline.
        .trigger(processingTime="10 seconds")
        .start()
    )
    # outputMode("append") emits only finalized window counts (rows that the watermark has closed).
    # foreachBatch(write_to_sinks) routes each micro-batch through your custom function instead of a built-in sink.
    # checkpointLocation tells Spark to store committed Kafka offsets and query state in ./checkpoints/analytics. On restart, Spark reads this directory to resume exactly where it left off.
    # trigger(processingTime="10 seconds") fires a micro-batch every 10 seconds, giving events time to accumulate for efficient processing.
    # awaitTermination() blocks the main thread so the streaming query keeps running until you press Ctrl+C.
    # You could write directly with writeStream.format("kafka") for a single Kafka sink. But foreachBatch gives you a standard batch DataFrame you can write to multiple destinations in one pass.

    print("Streaming query started. Press Ctrl+C to stop.")
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
