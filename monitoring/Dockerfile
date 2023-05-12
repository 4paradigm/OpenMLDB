FROM python:3.11-slim-bullseye

ENV OPENMLDB_EXPORTER_VERSION=0.6.0

RUN pip install --no-cache-dir openmldb-exporter==${OPENMLDB_EXPORTER_VERSION}

EXPOSE 8000

# --config.zk_root and --config.zk_path must provided by user
ENTRYPOINT [ "/usr/local/bin/openmldb-exporter", "--log.level=INFO" ]
