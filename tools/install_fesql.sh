#! /bin/sh
#
# install_fesql.sh
export JAVA_HOME=/depends/thirdparty/jdk1.8.0_141
cd fesql && ln -sf /depends/thirdparty thirdparty && mkdir -p build
cd build && cmake -DCMAKE_INSTALL_PREFIX=/depends/thirdparty -DCOVERAGE_ENABLE=OFF -DPYSDK_ENABLE=OFF -DJAVASDK_ENABLE=OFF -DEXPRIRMENT_ENABLE=OFF .. && make -j10 install


