FROM openjdk:11.0.13-jre-slim-bullseye

LABEL org.opencontainers.image.source https://github.com/4paradigm/OpenMLDB

RUN  apt-get update \
     && apt-get install -y --no-install-recommends libgomp1 curl binutils procps python3 python3-pip python3-numpy vim \
     && rm -rf /var/lib/apt/lists/* \
     && pip install --no-cache-dir py4j==0.10.9 numpy lightgbm tornado requests pandas openmldb xgboost==1.4.2
# if pip install read timed out, add '-i https://pypi.tuna.tsinghua.edu.cn/simple'

COPY init.sh /work/
COPY predict-taxi-trip-duration/script /work/taxi-trip/
COPY talkingdata-adtracking-fraud-detection /work/talkingdata/

ENV LANG=en_US.UTF-8
ENV SPARK_HOME=/work/openmldb/spark-3.2.1-bin-openmldbspark
ARG OPENMLDB_VERSION=0.6.3

COPY setup_openmldb.sh /
RUN /setup_openmldb.sh "${OPENMLDB_VERSION}"  && rm /setup_openmldb.sh

WORKDIR /work

CMD [ "/bin/bash" ]
