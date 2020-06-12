#! /bin/sh
#
# run_py3_ut.sh
WORKDIR=`pwd`
mkdir -p build && cd build/python
fesql_wheel=$(ls dist | grep .whl | head -1)
pip install ./dist/${fesql_wheel}

pip install nose
cd ${WORKDIR}/onebox && sh start_all.sh
sleep 20
cd ${WORKDIR}/python/test && nosetests --with-xunit
