#! /bin/sh
#
# compile_in_docker.sh

rm thirdparty
ln -sf thirdparty_mac thirdparty
ln -sf thirdparty_mac thirdparty
rm -rf build
mkdir -p build && cd build && cmake .. && make -j4 && make test

