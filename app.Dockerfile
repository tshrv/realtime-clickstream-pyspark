FROM apache/spark:3.5.9

WORKDIR /app

USER root

ARG SPARK_KAFKA_VERSION=3.5.9
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1

RUN curl -fsSL -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-${SPARK_KAFKA_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_KAFKA_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_KAFKA_VERSION}.jar \
 && curl -fsSL -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-${SPARK_KAFKA_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_KAFKA_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_KAFKA_VERSION}.jar \
 && curl -fsSL -o /opt/spark/jars/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar \
 && curl -fsSL -o /opt/spark/jars/commons-pool2-${COMMONS_POOL2_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# COPY .python-version .python-version
COPY pyproject.toml pyproject.toml
COPY uv.lock uv.lock
# RUN uv sync
RUN uv export --format requirements-txt --no-hashes -o requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY src src
COPY enrich_clickstream enrich_clickstream 

USER spark

# CMD ["/opt/spark/bin/spark-submit", "--master", "spark://spark-master:7077", "--conf", "spark.driver.host=clickstream-processor", "--conf", "spark.driver.bindAddress=0.0.0.0", "src/main.py"]
# Run the Spark job using spark-submit with the specified master and driver configurations

# generic image entrypoint for spark-submit
ENTRYPOINT ["/opt/spark/bin/spark-submit"]