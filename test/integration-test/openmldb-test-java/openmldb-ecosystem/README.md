# Ecosystem Test

## Kafka Test

Required:
1. OpenMLDB cluster, no need to create database or table
2. Kafka server and connect worker

ref https://openmldb.ai/docs/zh/main/integration/online_datasources/kafka_connector_demo.html, the kafka-connect-jdbc jar version should >= 10.5.0-SNAPSHOT-0.7.3(support auto schema).

Test steps:
1. No need to do creation(db, table, topic or else) in openmldb and kafka, just modify the config file `src/test/resources/kafka_test_cases.yml`. Check `connection.url`.
2. `mvn test -pl openmldb-ecosystem` on the root path
3. Feel free to re-run test
