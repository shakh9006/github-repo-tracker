FROM apache/spark:3.5.2-python3

USER root

RUN apt-get update && apt-get install -y \
    wget \
    curl \
    net-tools \
    iputils-ping \
    vim && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /home/spark/ && chown -R spark:spark /home/spark/

RUN curl -fsSL -o /tmp/miniforge.sh \
    https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-x86_64.sh && \
    bash /tmp/miniforge.sh -b -p /opt/conda && rm /tmp/miniforge.sh && \
    /opt/conda/bin/conda create -y -p /opt/conda/envs/py312 python=3.12.11 && \
    /opt/conda/bin/conda clean -afy

ENV PATH=/opt/conda/envs/py312/bin:/opt/conda/bin:$PATH
ENV PYSPARK_PYTHON=/opt/conda/envs/py312/bin/python

RUN mkdir -p /opt/spark/events

ENV SPARK_DRIVER_MEMORY=4g
ENV SPARK_EXECUTOR_MEMORY=4g
ENV SPARK_DRIVER_OPTS="-Xms512m -Xmx4g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"

USER spark

WORKDIR /opt/app

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt
