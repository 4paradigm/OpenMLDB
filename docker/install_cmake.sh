#! /bin/sh
#
# install_cmake.sh
wget https://github.com/Kitware/CMake/releases/download/v3.15.4/cmake-3.15.4.tar.gz
tar -zxvf cmake-3.15.4.tar.gz && cd cmake-3.15.4
./bootstrap && gmake -j4 && gmake install
