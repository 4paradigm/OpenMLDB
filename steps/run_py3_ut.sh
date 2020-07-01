#!/bin/bash
WORKDIR=$(pwd)
bash steps/gen_code.sh
mkdir -p build && cd build
cmake .. && make -j8 python_package
cd python && python3 -m pip install .
python3 -m pip install nose
cd ${WORKDIR}
cp steps/zoo.cfg thirdsrc/zookeeper-3.4.14/conf
cd thirdsrc/zookeeper-3.4.14
netstat -anp | grep 6181 | awk '{print $NF}' | awk -F '/' '{print $1}'| xargs kill -9
./bin/zkServer.sh start && cd ${WORKDIR}
sleep 5
cd onebox && sh start_onebox_on_rambuild.sh && cd ${WORKDIR}
#WORKDIR=${WORKDIR} bash ${WORKDIR}/python/test/create_table/drop_and_create.sh
cd ${WORKDIR}/python/test
nosetests --with-xunit
code=$?
cd ${WORKDIR}
cd onebox && sh stop_all.sh
cd ${WORKDIR}
cd thirdsrc/zookeeper-3.4.14 && ./bin/zkServer.sh stop
exit $code
