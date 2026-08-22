# Realtime Clickstream Pyspark
Real-time clickstream analytics pipeline using PySpark Structured Streaming and Apache Kafka.

- Real-Time Analytics with PySpark and Kafka
- Build a streaming pipeline that processes live clickstream events with PySpark.

## 30 Second Summary
Every click, scroll, and purchase on an e-commerce site generates a stream of events. Processing these events in real time is how engineering teams power live dashboards, detect anomalies, and trigger personalized recommendations within seconds of user activity.

In this project, you will build a real-time clickstream analytics pipeline using  and . You will produce simulated user events, deduplicate them, aggregate them into time windows with late-data handling, and write enriched results to multiple downstream sinks.

Pyspark structured streaming: Micro-batch by default (100ms+ typically), or experimental continuous mode

```bash
# create topic
docker exec kafka kafka-topics --create --topic clickstream_events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# create topic
docker exec kafka kafka-topics --create --topic clickstream_analytics --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# read events in a topic
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic clickstream_events --from-beginning
```