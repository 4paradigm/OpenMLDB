#! /bin/sh
#
# run_py3_ut.sh
WORKDIR=`pwd`
mkdir -p build && cd build 
cmake .. && make fesql_proto fesql_parser && make -j8 fesql python_package
cd python && pip install .
pip install nose
cd ${WORKDIR}/onebox && sh start_all.sh
sleep 20
cd ${WORKDIR}/python/test && nosetests --with-xunit
