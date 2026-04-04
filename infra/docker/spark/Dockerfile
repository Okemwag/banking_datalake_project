FROM apache/spark:3.5.7-scala2.12-java17-python3-ubuntu

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3-pip gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/project
COPY pyproject.toml README.md /opt/project/
COPY infra/docker/spark/requirements.txt /tmp/requirements.txt
COPY ingestion /opt/project/ingestion
COPY lakehouse /opt/project/lakehouse
COPY quality /opt/project/quality
COPY optimization /opt/project/optimization
COPY observability /opt/project/observability
COPY orchestration /opt/project/orchestration
COPY scripts /opt/project/scripts
COPY config /opt/project/config
RUN python3 -m pip install --no-cache-dir --upgrade pip setuptools wheel \
    && python3 -m pip install --no-cache-dir -r /tmp/requirements.txt \
    && python3 -m pip install --no-cache-dir --no-deps .
