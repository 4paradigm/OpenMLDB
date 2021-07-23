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

set -eE
set -o nounset

ROOT=$(git rev-parse --show-toplevel)
cd "$ROOT"

if [[ "$OSTYPE" = "darwin"* ]]; then
    echo "Install thirdparty for MacOS"

    # on local machine, one can tweak thirdparty path by passing extra argument
    THIRDPARTY_PATH=${1:-"$ROOT/thirdparty"}
    THIRDSRC_PATH="$ROOT/thirdsrc"

    echo "CICD_RUNNER_THIRDPARTY_PATH: ${THIRDPARTY_PATH}"
    echo "CICD_RUNNER_THIRDSRC_PATH: ${THIRDSRC_PATH}"

    mkdir -p "$THIRDPARTY_PATH"
    mkdir -p "$THIRDSRC_PATH"

    pushd "${THIRDSRC_PATH}"

    # download thirdparty-mac
    curl -SLo thirdparty-mac.tar.gz https://github.com/jingchen2222/hybridsql-asserts/releases/download/v0.1.4-pre2/thirdparty-2021-07-22-darwin.tar.gz
    tar xzf thirdparty-mac.tar.gz -C "${THIRDPARTY_PATH}" --strip-components 1
    # download and install libzetasql
    curl -SLo libzetasql.tar.gz https://github.com/jingchen2222/zetasql/releases/download/v0.2.0/libzetasql-0.2.0-darwin-x86_64.tar.gz
    tar xzf libzetasql.tar.gz -C "${THIRDPARTY_PATH}" --strip-components 1

    popd
elif [[ "$OSTYPE" = "linux-gnu"* ]]; then
    # unpack thirdparty first time
    pushd /depends
    # TODO: download thirdparty instead
    if [[ ! -d thirdparty && -r thirdparty.tar.gz ]]; then
        mkdir -p thirdparty
        tar xzf thirdparty.tar.gz -C thirdparty --strip-components=1
        curl -SL -o libzetasql.tar.gz https://github.com/jingchen2222/zetasql/releases/download/v0.2.0/libzetasql-0.2.0-linux-x86_64.tar.gz
        tar xzf libzetasql.tar.gz -C thirdparty --strip-components 1
        rm libzetasql.tar.gz
    fi
    popd

    ln -sf /depends/thirdparty thirdparty
fi
