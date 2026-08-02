FROM apache/spark:3.5.9

WORKDIR /app

USER root

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# COPY .python-version .python-version
COPY pyproject.toml pyproject.toml
COPY uv.lock uv.lock
# RUN uv sync
RUN uv export --format requirements-txt --no-hashes -o requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY src src

USER spark

CMD /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.driver.host=clickstream-processing-app src/main.py