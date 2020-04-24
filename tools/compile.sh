#! /bin/sh
#
# compile.sh
PWD=`pwd`

if $(uname -a | grep -q Darwin); then
    JOBS=$(sysctl -n machdep.cpu.core_count)
else
    JOBS=$(grep -c ^processor /proc/cpuinfo 2>/dev/null)
fi 

mkdir -p build && cd build 
cmake .. && make fesql_proto && make fesql_parser && make -j${JOBS}
