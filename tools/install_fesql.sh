#! /bin/sh
#
# install_fesql.sh

cd fesql && mkdir -p build
cd build && cmake -DCMAKE_INSTALL_PREFIX=/depends/thirdparty -DCOVERAGE_ENABLE=OFF .. && make -j10 install


