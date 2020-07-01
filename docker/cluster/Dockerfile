FROM conda/miniconda3-centos7:latest
USER root
RUN pip config set global.index-url https://mirrors.aliyun.com/pypi/simple/
RUN pip install lightgbm pyspark==2.3.4 numpy pandas sklearn tornado py4j requests
RUN yum install -y wget git
COPY ./bin /work/fedb/bin/
COPY ./conf /work/fedb/conf/
COPY ./start.sh /work/start.sh
COPY ./zookeeper-3.4.14 /work/zookeeper-3.4.14
COPY ./jdk1.8.0_121 /work/jdk1.8.0_121
COPY ./spark-2.3.4-bin-hadoop2.7 /work/spark-2.3.4-bin-hadoop2.7
COPY ./sql_pysdk /work/sql_pysdk
COPY ./fe_python /work/fe_python
RUN cp /work/fedb/bin/fedb /bin/
RUN cd /work/sql_pysdk && pip install .  && cd / && rm -rf /work/sql_pysdk
RUN cd /work/fe_python && pip install .  && cd / && rm -rf /work/fe_python
ENTRYPOINT ["/bin/bash", "/work/start.sh"]
