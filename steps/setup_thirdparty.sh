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
NC='\033[0m'
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
popd

THIRDPARTY_PATH=

function usage ()
{
    echo "Usage :  $0 [options] [--]

    Options:
    -h|help       Display this message
    -p            Directory for thirdpary install
    -f            Force download and install dependencies even file already exists.
    -z            Download and setup zetasql, if value is 0, zetasql setup will skipped. Default 1
"
}    # ----------  end of function usage  ----------

#-----------------------------------------------------------------------
#  Handle command line arguments
#-----------------------------------------------------------------------

# when _SETUP_ZETASQL is 0, script won't setup zetasql
_SETUP_ZETASQL=1
# when _FORCE_SETUP is set, script won't check if thirdparty path already exist
_FORCE_SETUP=
while getopts ":hfz:p:" opt
do
  case $opt in

    h)  usage; exit 0   ;;
    f) _FORCE_SETUP=1 ;;
    z) _SETUP_ZETASQL=$OPTARG ;;
    p) THIRDPARTY_PATH=$OPTARG ;;

    * )  echo -e "\n  Option does not exist : OPTARG\n"
                usage; exit 1   ;;

  esac    # --- end of case ---
done
shift $((OPTIND-1))

# on local machine, one can tweak thirdparty path by passing extra argument
if [ -z "$THIRDPARTY_PATH" ]; then
  THIRDPARTY_PATH=$ROOT/thirdparty
fi
if [[ -d "$THIRDPARTY_PATH" && -z $_FORCE_SETUP ]]; then
    echo "thirdparty path: $THIRDPARTY_PATH already exist, skip download deps"
    exit 0
fi

mkdir -p "$THIRDPARTY_PATH"

# get absolute path
THIRDPARTY_PATH=$(cd "$THIRDPARTY_PATH" 2> /dev/null && pwd)

pushd "$ROOT"
ARCH=$(arch)

THIRDPARTY_HOME=https://github.com/jingchen2222/hybridsql-asserts
ZETASQL_HOME=https://github.com/aceforeverd/zetasql
ZETASQL_VERSION=0.2.1.beta9

echo "Install thirdparty ... for $(uname -a)"

THIRDSRC_PATH="$ROOT/thirdsrc"

mkdir -p "$THIRDSRC_PATH"

pushd "${THIRDSRC_PATH}"

if [[ "$OSTYPE" = "darwin"* ]]; then
    THIRDPARY_URL="$THIRDPARTY_HOME/releases/download/v0.4.0/thirdparty-2021-08-03-darwin-x86_64.tar.gz"
    ZETASQL_URL="$ZETASQL_HOME/releases/download/v$ZETASQL_VERSION/libzetasql-$ZETASQL_VERSION-darwin-x86_64.tar.gz"
elif [[ "$OSTYPE" = "linux-gnu"* ]]; then
    if [[ $ARCH = 'x86_64' ]]; then
        THIRDPARY_URL="$THIRDPARTY_HOME/releases/download/v0.4.0/thirdparty-2021-08-03-linux-gnu-x86_64.tar.gz"
        ZETASQL_URL="$ZETASQL_HOME/releases/download/v$ZETASQL_VERSION/libzetasql-$ZETASQL_VERSION-linux-gnu-x86_64.tar.gz"
    elif [[ $ARCH = 'aarch64' ]]; then
        THIRDPARY_URL="$THIRDPARTY_HOME/releases/download/v0.4.0/thirdparty-2021-08-03-linux-gnu-aarch64.tar.gz"
        ZETASQL_URL="$ZETASQL_HOME/releases/download/v$ZETASQL_VERSION/libzetasql-$ZETASQL_VERSION-linux-gnu-aarch64.tar.gz"
    fi
fi

fetch "$THIRDPARY_URL" thirdparty.tar.gz
tar xzf "$THIRDSRC_PATH/thirdparty.tar.gz" -C "${THIRDPARTY_PATH}" --strip-components 1
if [[ $_SETUP_ZETASQL != '0' ]] ; then
    fetch "$ZETASQL_URL" libzetasql.tar.gz
    tar xzf "$THIRDSRC_PATH/libzetasql.tar.gz" -C "${THIRDPARTY_PATH}" --strip-components 1
fi

popd
popd
