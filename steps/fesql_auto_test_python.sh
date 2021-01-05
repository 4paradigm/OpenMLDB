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

#cd fesql
#git fetch
#git checkout feat/fix-python-some-fail-case
#git pull

cd ${ROOT_DIR}
cd ${ROOT_DIR}/build/python/dist/
whl_name=`ls | grep *.whl`
echo "whl_name:${whl_name}"
python3 -m pip install ${whl_name} -i https://pypi.tuna.tsinghua.edu.cn/simple

cd ${ROOT_DIR}
cd src/sdk/python/fesql-auto-test-python
python3 -m pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
sed -i "s/env=.*/env=cicd/" fesql.conf
#IP=`hostname -i`
IP=127.0.0.1
echo "cicd_tb_endpoint_0=$IP:9520" >> fesql.conf
echo "cicd_tb_endpoint_1=$IP:9521" >> fesql.conf
echo "cicd_tb_endpoint_2=$IP:9522" >> fesql.conf

cat fesql.conf
rm -rf report
mkdir report
python3 testMain.py
cd report
cat TEST-test_*.xml | grep "<testsuite"
if [ $? -ne 0 ] ;then
  exit 1
fi
err_count=`cat TEST-test_* | grep "<testsuite" | sed "s/<test.*errors=\"\([0-9]\+\)\".*>/\1/" | awk 'BEGIN{n=0}{n+=$1}END{print n}'`
echo "err_count:$err_count"
if [ $err_count -gt 0 ];then
  exit 1
else
  exit 0
fi
