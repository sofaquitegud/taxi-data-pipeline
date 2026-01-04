# Custom Airflow image with Java and Hadoop AWS for PySpark + MinIO
FROM apache/airflow:2.8.1-python3.11

USER root

# Install Java, PostgreSQL client, and wget
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    libpq-dev \
    gcc \
    wget \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Download Hadoop AWS jars for S3A filesystem support
RUN mkdir -p /opt/spark/jars \
    && wget -q -P /opt/spark/jars https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && wget -q -P /opt/spark/jars https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

ENV SPARK_EXTRA_JARS=/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

USER airflow

# Install Python dependencies
RUN pip install --no-cache-dir \
    apache-airflow-providers-amazon \
    boto3 \
    minio \
    pyspark==3.5.0 \
    great-expectations \
    psycopg2-binary \
    dbt-core==1.7.4 \
    dbt-postgres==1.7.4
