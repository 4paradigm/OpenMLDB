#!/usr/bin/env bash

ROOT_DIR=`pwd`
test -d /rambuild/ut_zookeeper && rm -rf /rambuild/ut_zookeeper/*
cp steps/zoo.cfg thirdsrc/zookeeper-3.4.14/conf
cd thirdsrc/zookeeper-3.4.14
netstat -anp | grep 6181 | awk '{print $NF}' | awk -F '/' '{print $1}'| xargs kill -9
./bin/zkServer.sh start && cd $ROOT_DIR
sleep 5
cd onebox && sh start_onebox_on_rambuild.sh && cd $ROOT_DIR
sleep 5
echo "ROOT_DIR:${ROOT_DIR}"


cd ${ROOT_DIR}
cd ${ROOT_DIR}/build/python/dist/
whl_name=`ls | grep *.whl | grep sqlalchemy_fedb`
echo "whl_name:${whl_name}"
python3 -m pip install ${whl_name} -i https://pypi.tuna.tsinghua.edu.cn/simple

cd ${ROOT_DIR}
python3 -m pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
cd -
cd ${ROOT_DIR}/src/sdk/python/sqlalchemy-test && nosetests --with-xunit

