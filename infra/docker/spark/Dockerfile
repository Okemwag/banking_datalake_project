FROM bitnami/spark:3.5.1

USER root
RUN install_packages python3-pip gcc

WORKDIR /opt/project
COPY pyproject.toml README.md /opt/project/
COPY ingestion /opt/project/ingestion
COPY lakehouse /opt/project/lakehouse
COPY quality /opt/project/quality
COPY optimization /opt/project/optimization
COPY observability /opt/project/observability
COPY orchestration /opt/project/orchestration
COPY scripts /opt/project/scripts
COPY config /opt/project/config
RUN pip3 install --no-cache-dir -e .

USER 1001

