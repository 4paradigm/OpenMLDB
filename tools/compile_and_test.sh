#! /bin/sh
#
# compile.sh
PWD=`pwd`
PWD=`pwd`

if $(uname -a | grep -e Darwin); then
    JOBS=$(sysctl -n machdep.cpu.core_count)
else
    JOBS=$(grep -c ^processor /proc/cpuinfo 2>/dev/null)
fi

export PATH=${PWD}/thirdparty/bin:$PATH
rm -rf build
mkdir -p build && cd build && cmake .. && make -j${JOBS} && make test
