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
set -e

echo "Install thirdparty for MacOS"
echo "CICD_RUNNER_THIRDPARTY_PATH: ${CICD_RUNNER_THIRDPARTY_PATH}"
echo "CICD_RUNNER_THIRDSRC_PATH: ${CICD_RUNNER_THIRDSRC_PATH}"

mkdir "${CICD_RUNNER_THIRDPARTY_PATH}"
mkdir "${CICD_RUNNER_THIRDSRC_PATH}"
pushd "${CICD_RUNNER_THIRDSRC_PATH}"

# download thirdparty-mac
wget -nv --show-progress https://github.com/jingchen2222/hybridsql-asserts/releases/download/0.1/thirdparty-mac.tar.gz
tar xzf thirdparty-mac.tar.gz -C  "${CICD_RUNNER_THIRDPARTY_PATH}" --strip-components 1
echo "list files under ${CICD_RUNNER_THIRDPARTY_PATH}"

if [ -f "bison_succ" ]; then
  echo "bison exist"
else
  wget --no-check-certificate -O bison-3.4.tar.gz http://ftp.gnu.org/gnu/bison/bison-3.4.tar.gz
  tar zxf bison-3.4.tar.gz
  cd bison-3.4
  ./configure --prefix="${CICD_RUNNER_THIRDPARTY_PATH}" && make install
  cd -
  touch bison_succ
fi
popd