# Copyright 2021 4Paradigm
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#! /bin/sh
#
# compile.sh
set -e

WORK_DIR=`pwd`
if [ "$1" = "DEBUG" ]
then
    sed -i '/set(CMAKE_BUILD_TYPE/c\set(CMAKE_BUILD_TYPE Debug)' CMakeLists.txt 
else
    sed -i '/set(CMAKE_BUILD_TYPE/c\set(CMAKE_BUILD_TYPE RelWithDebInfo)' CMakeLists.txt 
fi

VERSION=$(git tag --points-at HEAD)
VERSION=${VERSION:1}
if [ -n "${VERSION}" ]; then
    if [[ ! ($VERSION =~ ^[0-9]{1,2}\.[0-9]{1,2}\.[0-9]{1,2}\.[0-9]{1,2}$) ]]; then
        echo "$VERSION is not release version"
        exit 1
    fi
    sh ./steps/release.sh ${VERSION}
fi

mkdir -p $WORK_DIR/build  || :
cd $WORK_DIR/build && cmake .. && make -j8
code=$?
cd $WORK_DIR
exit $code
