FROM apache/airflow:3.1.7

COPY requirements.txt /requirements.txt

USER root

RUN apt-get update && apt-get install -y \
    build-essential \
    wget \
    curl \
    net-tools \
    iputils-ping \
    vim \
    && rm -rf /var/lib/apt/lists/*

RUN apt-get update && apt-get install -y openjdk-17-jre-headless procps && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Preload Spark runtime jars once at image build time.
RUN mkdir -p /opt/spark/jars/custom \
    && cd /opt/spark/jars/custom \
    && wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.9.2/iceberg-spark-runtime-3.5_2.12-1.9.2.jar \
    && wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.9.2/iceberg-aws-bundle-1.9.2.jar \
    && wget -q https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/0.102.5/nessie-spark-extensions-3.5_2.12-0.102.5.jar \
    && wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    && chown -R airflow:0 /opt/spark/jars

USER airflow

RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt

RUN pip uninstall -y pyspark py4j || true
RUN pip install --no-cache-dir pyspark==3.5.2 py4j==0.10.9.7
