FROM centos:centos6

USER root
COPY ./bin /work/rtidb/bin/
COPY ./conf /work/rtidb/conf/

COPY ./start.sh /work/start.sh

COPY ./zookeeper-3.4.14 /work/zookeeper-3.4.14
COPY ./jdk1.8.0_121 /work/jdk1.8.0_121

VOLUME ["/work/rtidb/db", "/work/rtidb/logs", "/work/rtidb/recycle"]

ENTRYPOINT ["/bin/bash", "/work/start.sh"]
