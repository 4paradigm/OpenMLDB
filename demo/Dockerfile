FROM openjdk:11.0.13-jre-slim-bullseye

LABEL org.opencontainers.image.source https://github.com/4paradigm/OpenMLDB

# if 'apt update' is too slow, change the sources
COPY sources.list /etc/apt/
RUN  apt-get update \
     && apt-get install -y --no-install-recommends libgomp1 curl binutils procps python3 python3-pip python3-numpy vim rsync \
     && rm -rf /var/lib/apt/lists/* \
     && pip install --no-cache-dir py4j==0.10.9 numpy lightgbm tornado requests pandas openmldb xgboost==1.4.2 -i https://pypi.tuna.tsinghua.edu.cn/simple
# if pip install read timed out, add '-i https://pypi.tuna.tsinghua.edu.cn/simple'
# can't copy files from outside, so copy whl to current path
COPY openmldb*.whl /
RUN pip install openmldb*.whl --force-reinstall -i https://pypi.tuna.tsinghua.edu.cn/simple

COPY init.sh /work/
COPY predict-taxi-trip-duration/script /work/taxi-trip/
COPY talkingdata-adtracking-fraud-detection /work/talkingdata/

ENV LANG=en_US.UTF-8
ENV SPARK_HOME=/work/openmldb/spark-3.2.1-bin-openmldbspark
ARG OPENMLDB_VERSION=0.6.9
ENV OPENMLDB_VERSION="${OPENMLDB_VERSION}"

COPY setup_openmldb.sh /
# You can copy the downloaded pacakges here, and use skip_download in setup_openmldb.sh
COPY *gz / 
RUN /setup_openmldb.sh "${OPENMLDB_VERSION}" skip_download && rm /setup_openmldb.sh

WORKDIR /work

CMD [ "/bin/bash" ]
