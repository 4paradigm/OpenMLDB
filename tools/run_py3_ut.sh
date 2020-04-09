#! /bin/sh
#
# run_py3_ut.sh

WORKDIR=`pwd`
export JAVA_HOME=${WORKDIR}/thirdparty/jdk1.8.0_141
export PATH=${WORKDIR}/thirdparty/bin:$JAVA_HOME/bin:${WORKDIR}/thirdparty/apache-maven-3.6.3/bin:$PATH
mkdir -p build && cd build 
cmake .. && make fesql_proto fesql_parser && make -j20 fesql python_package
cd python && pip install .
pip install nose
cd ${WORKDIR}/onebox && sh start_all.sh
sleep 20
cd ${WORKDIR}/python/test && nosetests --with-xunit

