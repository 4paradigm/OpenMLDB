FROM centos:8

LABEL org.opencontainers.image.source https://github.com/4paradigm/OpenMLDB

RUN yum install -y python38 python38-pip python38-numpy libgomp java-1.8.0-openjdk-headless && \
    yum clean all && \
    pip3 install lightgbm tornado requests pandas openmldb

COPY demo /work/taxi-trip/
COPY develop_ml_application_tour.ipynb /work/

ENV LANG=en_US.UTF-8

COPY setup_openmldb.sh /
RUN /setup_openmldb.sh && rm /setup_openmldb.sh

WORKDIR /work/taxi-trip
