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

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${RED}this script is deprecated, please run 'make thirdparty' in top directory instead${NC}"

fetch() {
    if [ $# -ne 2 ]; then
        echo "usage: fetch url output_file"
        exit 1
    fi
    local url=$1
    local file_name=$2
    if [ ! -e "$file_name" ]; then
        echo -e "${GREEN}downloading $url ...${NC}"
        curl -SL -o "$file_name" "$url"
        echo -e "${GREEN}download $url${NC}"
    else
        echo "$file_name" already downloaded
    fi
}

pushd "$(dirname "$0")/.."
ROOT=$(pwd)

ARCH=$(arch)

THIRDPARTY_HOME=https://github.com/4paradigm/hybridsql-asserts
ZETASQL_HOME=https://github.com/4paradigm/zetasql
ZETASQL_VERSION=0.2.2

echo "Install thirdparty ... for $(uname -a)"

# on local machine, one can tweak thirdparty path by passing extra argument
THIRDPARTY_PATH=${1:-"$ROOT/thirdparty"}
THIRDSRC_PATH="$ROOT/thirdsrc"

if [ -d "$THIRDPARTY_PATH" ]; then
    echo "thirdparty path: $THIRDPARTY_PATH already exist, skip download deps"
    exit 0
fi

mkdir -p "$THIRDPARTY_PATH"
mkdir -p "$THIRDSRC_PATH"

pushd "${THIRDSRC_PATH}"
# TODO: show download link, so user can copy link easily
if [[ "$OSTYPE" = "darwin"* ]]; then
    fetch "$THIRDPARTY_HOME/releases/download/v0.4.0/thirdparty-2021-08-03-darwin-x86_64.tar.gz" thirdparty.tar.gz
    fetch "$ZETASQL_HOME/releases/download/v$ZETASQL_VERSION/libzetasql-$ZETASQL_VERSION-darwin-x86_64.tar.gz" libzetasql.tar.gz
elif [[ "$OSTYPE" = "linux-gnu"* ]]; then
    if [[ $ARCH = 'x86_64' ]]; then
        fetch "$THIRDPARTY_HOME/releases/download/v0.4.0/thirdparty-2021-08-03-linux-gnu-x86_64.tar.gz" thirdparty.tar.gz
        fetch "$ZETASQL_HOME/releases/download/v$ZETASQL_VERSION/libzetasql-$ZETASQL_VERSION-linux-gnu-x86_64.tar.gz" libzetasql.tar.gz
    elif [[ $ARCH = 'aarch64' ]]; then
        fetch "$THIRDPARTY_HOME/releases/download/v0.4.0/thirdparty-2021-08-03-linux-gnu-aarch64.tar.gz" thirdparty.tar.gz
        fetch "$ZETASQL_HOME/releases/download/v$ZETASQL_VERSION/libzetasql-$ZETASQL_VERSION-linux-gnu-aarch64.tar.gz" libzetasql.tar.gz
    fi
fi
popd

tar xzf "$THIRDSRC_PATH/thirdparty.tar.gz" -C "${THIRDPARTY_PATH}" --strip-components 1
tar xzf "$THIRDSRC_PATH/libzetasql.tar.gz" -C "${THIRDPARTY_PATH}" --strip-components 1
popd
