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
#sh steps/gen_code.sh
#sh tools/install_fesql.sh
#mkdir -p ${ROOT_DIR}/build  && cd ${ROOT_DIR}/build && cmake .. && make -j16 python_package
#cd ${ROOT_DIR}/python && python3 -m pip install .

#cd fesql
#git checkout feat/add-mode-python-unsupport
#git pull

cd ${ROOT_DIR}/build/sql_pysdk/dist/
whl_name=`ls | grep *.whl`
echo "whl_name:${whl_name}"
python3 -m pip install ${whl_name}

cd ${ROOT_DIR}
cd src/sdk/python/fesql-auto-test-python
python3 -m pip install -r requirements.txt
sed -i "s/env=.*/env=cicd/" fesql.conf
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
