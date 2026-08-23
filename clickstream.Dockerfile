FROM apache-spark:3.5.9

USER root

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# COPY .python-version .python-version
COPY pyproject.toml pyproject.toml
COPY uv.lock uv.lock
# RUN uv sync
RUN uv export --format requirements-txt --no-hashes -o requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY clickstream_processor clickstream_processor
COPY enrich_clickstream enrich_clickstream 

USER spark

# CMD ["/opt/spark/bin/spark-submit", "--master", "spark://spark-master:7077", "--conf", "spark.driver.host=clickstream-processor", "--conf", "spark.driver.bindAddress=0.0.0.0", "clickstream_processor/main.py"]
# Run the Spark job using spark-submit with the specified master and driver configurations

# generic image entrypoint for spark-submit
ENTRYPOINT ["/opt/spark/bin/spark-submit"]