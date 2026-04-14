FROM bitnamilegacy/spark:3.5.6 AS spark-base

FROM apache/airflow:3.1.6

USER root

# Install OpenJDK (JRE), build dependencies, and curl for Spark download
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    build-essential \
    librdkafka-dev \
    libffi-dev \
    libssl-dev \
    curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set Java paths
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# ============================================================
# Copy Spark binaries from cluster image
# This guarantees exact version alignment
# ============================================================
COPY --from=spark-base /opt/bitnami/spark /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# ============================================================
# Download Iceberg + AWS JARs (same versions as Spark cluster)
# Required for SparkSubmitOperator to work with Iceberg tables
# ============================================================
RUN set -eux; \
    curl -fSL -o "${SPARK_HOME}/jars/iceberg-spark-runtime-3.5_2.12-1.9.2.jar" \
        "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.9.2/iceberg-spark-runtime-3.5_2.12-1.9.2.jar"; \
    curl -fSL -o "${SPARK_HOME}/jars/iceberg-aws-bundle-1.9.2.jar" \
        "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.9.2/iceberg-aws-bundle-1.9.2.jar"; \
    curl -fSL -o "${SPARK_HOME}/jars/hadoop-aws-3.3.4.jar" \
        "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"; \
    curl -fSL -o "${SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.791.jar" \
        "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.791/aws-java-sdk-bundle-1.12.791.jar"; \
    curl -fSL -o "${SPARK_HOME}/jars/clickhouse-jdbc-0.6.4-all.jar" \
        "https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.4/clickhouse-jdbc-0.6.4-all.jar"


# Copy entrypoint script as root and set permissions
COPY entrypoint.sh /entrypoint.sh
RUN chmod 755 /entrypoint.sh

USER airflow

# Copy and install requirements
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt