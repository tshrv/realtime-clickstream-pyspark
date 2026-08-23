"""
This script reads two live streams (clickstream events and user profiles), joins them on user_id with a time constraint, and writes enriched results to the enriched_events topic.
"""

from contextlib import contextmanager

from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, from_json, struct, to_json
from pyspark.sql.types import StringType, StructField, StructType, TimestampType


@contextmanager
def get_spark_session():
    spark = (
        SparkSession.builder.appName("EnrichedClickstream")
        .config("spark.sql.streaming.schemaInference", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"Running spark version: {spark.version}")
    yield spark
    spark.stop()

def enrich_clickstream_data(spark: SparkSession):
    # Clickstream schema
    click_schema = StructType(
        [
            StructField("event_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("page", StringType(), True),
        ]
    )

    # Profile schema
    profile_schema = StructType(
        [
            StructField("user_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("segment", StringType(), True),
            StructField("profile_timestamp", StringType(), True),
        ]
    )

    # Read clickstream
    clicks_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:29092")
        .option("subscribe", "clickstream_events")
        .option("startingOffsets", "latest")
        .load()
    )

    clicks = (
        clicks_raw.selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), click_schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", col("timestamp").cast(TimestampType()))
        .withWatermark("event_time", "2 minutes")
    ).alias("clicks")
    # Reads from the clickstream-events topic and parses each message's value into structured columns.
    # Casts the string timestamp to a proper TimestampType column called event_time.
    # The 2-minute watermark tells Spark that clickstream events arriving more than 2 minutes late can be dropped from join state.

    # Read profiles
    profiles_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:29092")
        .option("subscribe", "user_profiles")
        .option("startingOffsets", "latest")
        .load()
    )

    profiles = (
        profiles_raw.selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), profile_schema).alias("data"))
        .select("data.*")
        .withColumn(
            "profile_time", col("profile_timestamp").cast(TimestampType())
        )
        .withWatermark("profile_time", "5 minutes")
    ).alias("profiles")
    # Profile updates are less frequent (one every 2 seconds vs 5 clickstream events per second). A 5-minute watermark gives Spark a wider window to buffer profile rows so they can be matched against click events that arrive slightly later. The asymmetric watermarks reflect the different arrival rates of the two streams.

    # Stream-stream join with time constraint
    enriched = clicks.join(
        profiles,
        expr(
            """
            clicks.user_id = profiles.user_id AND
            event_time >= profile_time AND
            event_time <= profile_time + interval 10 minutes
            """
        ),
    )

    # Select enriched fields
    output = enriched.select(
        clicks.event_id,
        clicks.user_id,
        clicks.event_type,
        clicks.event_time,
        clicks.page,
        profiles.name,
        profiles.segment,
    )
    # The expression event_time >= profile_time AND event_time <= profile_time + interval 10 minutes means a click event only joins with a profile update if it happened within 10 minutes after the profile was emitted.
    # This constraint is what allows Spark to clean up old state. Without it, Spark would have to buffer every profile row forever in case a late click event matches it. With the constraint, Spark knows it can discard a profile row once the watermark has advanced past its 10-minute window.

    # Write to Kafka
    kafka_output = output.select(
        to_json(
            struct(
                col("event_id"),
                col("user_id"),
                col("event_type"),
                col("event_time"),
                col("page"),
                col("name"),
                col("segment"),
            )
        ).alias("value")
    )

    query = (
        kafka_output.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:29092")
        .option("topic", "enriched_events")
        .option("checkpointLocation", "/checkpoints/enriched")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )
    # Serializes the enriched output (event details plus user name and segment) into a JSON value column that Kafka expects.
    # Writes in append mode, which is the only supported output mode for stream-stream joins.
    # Checkpoints to ./checkpoints/enriched so the query can resume from where it left off after a restart.
    
    print("Enriched stream started. Check Spark UI at http://localhost:4040")
    query.awaitTermination()

def main():
    logger.info("Starting enriched stream consumer...")
    with get_spark_session() as spark:
        logger.info("Spark session created successfully.")
        # Add your Spark processing logic here
        logger.info("Processing started")
        enrich_clickstream_data(spark)
        logger.info("Processing stopped")


if __name__ == "__main__":
    main()
