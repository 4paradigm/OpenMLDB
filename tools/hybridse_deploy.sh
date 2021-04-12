#!/bin/bash

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

# hybridse_deploy.sh create compiled archive from make install

set -eE

# goto toplevel directory
pushd "$(dirname "$0")/.."

if [[ -n $VERSION ]]; then
    HYBRIDSE_VERSION=$VERSION
else
    HYBRIDSE_VERSION="SNAPSHOT-$(date +%Y-%m-%d)"
fi

if uname -a | grep -q Darwin; then
    # in case coreutils not install on mac
    alias nproc='sysctl -n hw.logicalcpu'
fi

mkdir -p build
pushd build/

cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX="hybridse"
make -j "$(nproc)" install
mv hybridse "hybridse-$HYBRIDSE_VERSION"
tar czf ../hybridse-"$HYBRIDSE_VERSION.tar.gz" "hybridse-$HYBRIDSE_VERSION"

popd

popd
