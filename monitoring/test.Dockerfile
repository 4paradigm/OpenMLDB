FROM python:3.11-slim-bullseye

RUN pip install --no-cache-dir poetry

WORKDIR /usr/src/openmldb-exporter

CMD [ "/bin/bash" ]
