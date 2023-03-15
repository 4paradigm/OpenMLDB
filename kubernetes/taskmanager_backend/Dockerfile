FROM gcr.io/spark-operator/spark:v3.1.1
 
USER root
 
RUN rm /opt/spark/jars/*
ADD ./spark-3.2.1-bin-openmldbspark/jars/* /opt/spark/jars/

ENTRYPOINT ["/opt/entrypoint.sh"]
