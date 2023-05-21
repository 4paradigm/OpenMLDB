# Ecosystem Test

## Kafka Test

Required:
1. OpenMLDB cluster and create database(`kafka_test`)
2. Kafka server and connect worker

ref https://openmldb.ai/docs/zh/main/integration/online_datasources/kafka_connector_demo.html, the kafka-connect-jdbc jar version should >= 10.5.0-SNAPSHOT-0.7.3(support auto schema).

Test steps:
1. No need to do creation(topic or else) in kafka, just check the `connection.url` in `src/test/resources/kafka_test_cases.yml`, the database should be created
2. modify `src/test/resources/kafka.properties`, if you use custom apiserver, kafka server or kafka connect worker
3. `mvn test -pl openmldb-ecosystem` on the root path
4. If re-run, you should drop all tables in `kafka_test`
