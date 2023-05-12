# openmldb exporter image from source code

FROM python:3.11-slim-bullseye

RUN pip install --no-cache-dir poetry

WORKDIR /usr/src/openmldb-exporter

COPY ./ /usr/src/openmldb-exporter

RUN poetry install

EXPOSE 8000

# --config.zk_root and --config.zk_path must provided by user
ENTRYPOINT [ "poetry", "run", "openmldb-exporter" ]
