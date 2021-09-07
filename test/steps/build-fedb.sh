#!/usr/bin/env bash

ROOT_DIR=`pwd`
sh steps/clone-fedb.sh
cd OpenMLDB
ls -al
sh ${ROOT_DIR}/steps/retry-command.sh "bash steps/init_env.sh"
mkdir -p build
source /root/.bashrc && cd build && cmake -DSQL_PYSDK_ENABLE=ON -DSQL_JAVASDK_ENABLE=ON -DTESTING_ENABLE=ON .. && make -j$(nproc)
make sqlalchemy_fedb && cd ../
cd src/sdk/java
mvn clean install -Dmaven.test.skip=true
