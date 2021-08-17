#!/bin/bash
#
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

# init_env.sh
set -eE

pushd "$(dirname "$0")/.."

ROOT=$(pwd)
ARCH=$(arch)
# hybridse is required downloaded or install locally
HYBRIDSE_SOURCE=$1

# on local machine, one can tweak thirdparty path by passing extra argument
THIRDPARTY_PATH=${2:-"$ROOT/thirdparty"}
THIRDSRC_PATH="$ROOT/thirdsrc"

echo "THIRDPARTY_PATH: "${THIRDPARTY_PATH}
echo "Install thirdparty ... for $(uname -a)"

if [[ -d "$THIRDPARTY_PATH" ]]; then
    echo "thirdparty path: $THIRDPARTY_PATH already exist, skip download deps"
else
    ./steps/setup_thirdparty.sh ${THIRDPARTY_PATH}
fi

if [[ -d "THIRDSRC_PATH" ]]; then
    echo "thirdsrc path: THIRDSRC_PATH already exist, skip download deps"
else
    curl -SLo thirdsrc.tar.gz https://github.com/jingchen2222/hybridsql-asserts/releases/download/v0.4.0/thirdsrc-2021-08-03.tar.gz
    tar xzf thirdsrc.tar.gz -C "${THIRDSRC_PATH}" --strip-components 1
fi

if [ -d "$THIRDPARTY_PATH/hybridse" ]; then
    echo "thirdparty/hybridse path: $THIRDPARTY_PATH/hybridse already exist, skip download/install deps"
    exit 0
fi
echo "HYBRIDSE_SOURCE: "${HYBRIDSE_SOURCE}
if [[ ${HYBRIDSE_SOURCE} = "local" ]]; then
  echo "Install hybridse locally"
  cd "${ROOT}/hybridse"
  ln -sf $THIRDPARTY_PATH thirdparty
  ln -sf $THIRDSRC_PATH thirdsrc
  if uname -a | grep -q Darwin; then
    # in case coreutils not install on mac
    alias nproc='sysctl -n hw.logicalcpu'
  fi
  rm -rf build
  mkdir -p build && cd build
  cmake .. -DCMAKE_BUILD_TYPE=Release -DTESTING_ENABLE=OFF -DEXAMPLES_ENABLE=OFF -DCMAKE_INSTALL_PREFIX="hybridse"
  make -j"$(nproc)" install
  mv hybridse ${THIRDPARTY_PATH}/hybridse
else
  echo "Download hybridse package"
  pushd "${THIRDSRC_PATH}"

  if [[ "$OSTYPE" = "darwin"* ]]; then
    curl -SLo hybridse.tar.gz https://github.com/4paradigm/HybridSE/releases/download/v0.2.1/hybridse-0.2.1-darwin-x86_64.tar.gz
  elif [[ "$OSTYPE" = "linux-gnu"* ]]; then
    if [[ $ARCH = 'x86_64' ]]; then
        curl -SLo hybridse.tar.gz https://github.com/4paradigm/HybridSE/releases/download/v0.2.1/hybridse-0.2.1-linux-x86_64.tar.gz
    elif [[ $ARCH = 'aarch64' ]]; then
        # NOTE: missing hybridse-aarch64
        echo "missing hybridse-aarch64"
    fi
  fi
  mkdir -p "$THIRDPARTY_PATH/hybridse"
  tar xzf hybridse.tar.gz -C "${THIRDPARTY_PATH}/hybridse" --strip-components 1
  popd
fi
cd ${ROOT}

