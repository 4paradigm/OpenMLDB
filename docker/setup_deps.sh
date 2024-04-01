#!/bin/bash
# Copyright 2022 4Paradigm
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

INPUT=$(arch)
ZETASQL_VERSION=
THIRDPARTY_VERSION=

#===  FUNCTION  ================================================================
#         NAME:  usage
#  DESCRIPTION:  Display usage information.
#===============================================================================
function usage ()
{
    echo "Usage :  $0 [options] [--]

    Options:
    -h       Display this message
    -a       Specify os architecture, default $(arch)
    -t       hybridsql thirdparty version, required
    -z       Specify zetasql version, required"

}    # ----------  end of function usage  ----------

#-----------------------------------------------------------------------
#  Handle command line arguments
#-----------------------------------------------------------------------

while getopts ":ha:z:t:" opt
do
  case $opt in

    h)  usage; exit 0   ;;

    a)  INPUT=$OPTARG ;;

    t) THIRDPARTY_VERSION=$OPTARG ;;

    z)  ZETASQL_VERSION=$OPTARG ;;

    *)  echo -e "\n  Option does not exist : $OPTARG\n"
          usage; exit 1   ;;

  esac    # --- end of case ---
done
shift $((OPTIND-1))

if [[ -z "$ZETASQL_VERSION" || -z "$THIRDPARTY_VERSION" ]]; then
    echo "ZETASQL_VERSION and THIRDPARTY_VERSION number required"
    exit 1
fi

if [[ $INPUT = 'i386' || $INPUT = 'x86_64' || $INPUT = 'amd64' ]]; then
    ARCH=x86_64
elif [[ $INPUT = 'aarch64' || $INPUT = 'arm64' ]]; then
    ARCH=aarch64
else
    echo "Unsupported arch: $INPUT"
    exit 1
fi

pushd "$(dirname "$0")"

curl -Lo cmake.tar.gz https://github.com/Kitware/CMake/releases/download/v3.21.0/cmake-3.21.0-linux-"$ARCH".tar.gz && \
    echo "downloaded cmake.tar.gz for $ARCH"
tar xzf cmake.tar.gz -C /usr/local/ --strip-components=1
rm -v cmake.tar.gz

mkdir -p /deps/usr

if [[ "$ARCH" = "x86_64" ]]; then
    curl -Lo thirdparty.tar.gz "https://github.com/4paradigm/hybridsql-asserts/releases/download/v${THIRDPARTY_VERSION}/thirdparty-${THIRDPARTY_VERSION}-linux-gnu-x86_64-centos.tar.gz" && \
        echo "downloaded thirdparty.tar.gz version $THIRDPARTY_VERSION for $ARCH"
    curl -Lo zetasql.tar.gz "https://github.com/4paradigm/zetasql/releases/download/v${ZETASQL_VERSION}/libzetasql-${ZETASQL_VERSION}-linux-gnu-x86_64-centos.tar.gz" && \
        echo "downloaed zetasql.tar.gz version $ZETASQL_VERSION for $ARCH"
elif [[ "$ARCH" = "aarch64" ]]; then
    curl -Lo thirdparty.tar.gz "https://github.com/4paradigm/hybridsql-asserts/releases/download/v${THIRDPARTY_VERSION}/thirdparty-${THIRDPARTY_VERSION}-linux-gnu-${ARCH}.tar.gz" && \
        echo "downloaded thirdparty.tar.gz version $THIRDPARTY_VERSION for $ARCH"
    curl -Lo zetasql.tar.gz "https://github.com/4paradigm/zetasql/releases/download/v${ZETASQL_VERSION}/libzetasql-${ZETASQL_VERSION}-linux-gnu-${ARCH}.tar.gz" && \
        echo "downloaed zetasql.tar.gz version $ZETASQL_VERSION for $ARCH"
else
    echo "no pre-compiled deps for arch=$ARCH"
    exit 1
fi

tar xzf thirdparty.tar.gz -C /deps/usr --strip-components=1
tar xzf zetasql.tar.gz -C /deps/usr --strip-components=1
rm -v ./*.tar.gz

popd
