#!/bin/bash
# File   : setup_cmake.sh

set -eE

INPUT=${1:-$(arch)}

if [[ $INPUT = 'i386' || $INPUT = 'x86_64' || $INPUT = 'amd64' ]]; then
    ARCH=x86_64
elif [[ $INPUT = 'aarch64' || $INPUT = 'arm64' ]]; then
    ARCH=aarch64
else
    echo "Unsupported arch: $INPUT"
    exit 1
fi


curl -SLo cmake.tar.gz https://github.com/Kitware/CMake/releases/download/v3.21.0/cmake-3.21.0-linux-"$ARCH".tar.gz
tar xzf cmake.tar.gz -C /usr/local/ --strip-components=1
rm cmake.tar.gz
