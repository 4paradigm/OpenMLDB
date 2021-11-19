# https://github.com/docker-library/openjdk/blob/61bfbb30ae5a0395f08d41107b86bcce90d70ba6/11/jre/slim-bullseye/Dockerfile
FROM openjdk:11.0.13-jre-slim-bullseye

LABEL org.opencontainers.image.source https://github.com/4paradigm/OpenMLDB

RUN  apt-get update \
     && apt-get install -y --no-install-recommends libgomp1 curl binutils procps python3 python3-pip python3-numpy \
     && rm -rf /var/lib/apt/lists/* \
     && pip install --no-cache-dir numpy lightgbm tornado requests pandas openmldb

COPY demo /work/taxi-trip/
COPY develop_ml_application_tour.ipynb /work/

ENV LANG=en_US.UTF-8
ARG OPENMLDB_VERSION

COPY setup_openmldb.sh /
RUN /setup_openmldb.sh "${OPENMLDB_VERSION}"  && rm /setup_openmldb.sh

WORKDIR /work/taxi-trip
