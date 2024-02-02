#! /bin/bash

apiserver="$(awk -F '"' 'NR==7{print $2}' ../../../out/openmldb_info.yaml)"
zkc="zk=""$(awk -F '"' 'NR==2{print $2}' ../../../out/openmldb_info.yaml)"
zkpath="\&zkPath=""$(awk -F '"' 'NR==3{print $2}' ../../../out/openmldb_info.yaml)"
echo "${zkc}"
echo "${zkpath}"
zk="${zkc}""${zkpath}"

docker exec -it kafka_test /start.sh "${zk}"
sed -i "s#\"bootstrap.servers\":.*#\"bootstrap.servers\":node-4:9092,#" openmldb-ecosystem/src/test/resources/kafka_test_cases.yml
sed -i "s#\"connect.listeners\":.*#\"connect.listeners\":http://node-4:8083,#" openmldb-ecosystem/src/test/resources/kafka_test_cases.yml
sed -i "s#apiserver.address:.*#apiserver.address: "${apiserver}"#" openmldb-ecosystem/src/test/resources/kafka_test_cases.yml
sed -i "s#kafka_test?.*#kafka_test?"${zk}"\"#" openmldb-ecosystem/src/test/resources/kafka_test_cases.yml

mvn test -pl openmldb-ecosystem

docker exec -it kafka_test /stop.sh
