FROM apache/airflow:2.10.5-python3.11

ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.11
ARG AIRFLOW_CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

USER root
RUN apt-get update && apt-get install -y openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/*

USER airflow
WORKDIR /opt/project
COPY infra/docker/airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --default-timeout=100 --constraint "${AIRFLOW_CONSTRAINTS_URL}" -r /tmp/requirements.txt
COPY . /opt/project
RUN pip install --no-cache-dir --no-deps -e /opt/project

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV AIRFLOW__CORE__DAGS_FOLDER=/opt/project/orchestration/dags
