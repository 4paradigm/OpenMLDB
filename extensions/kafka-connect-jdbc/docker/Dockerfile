FROM openjdk:11

RUN curl -o kafka.tgz https://archive.apache.org/dist/kafka/3.1.0/kafka_2.13-3.1.0.tgz
RUN tar xzf ./kafka.tgz
RUN mv kafka_2.13-3.1.0 /kafka/

RUN curl --insecure -o kafka-connect-jdbc.tgz https://www.openmldb.com/download/use_case/kafka_connector/kafka-connect-jdbc.tgz
RUN tar xzf ./kafka-connect-jdbc.tgz
RUN mv ./kafka-connect-jdbc/ /tmp/

WORKDIR /

CMD ["/kafka/bin/connect-standalone.sh", "/kafka_config/connect-standalone.properties", "/kafka_config/openmldb-sink.properties"]
