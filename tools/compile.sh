#! /bin/sh
#
# compile.sh

mkdir build && cd build && cmake .. && make -j4 && make test

