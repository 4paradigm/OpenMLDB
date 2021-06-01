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

# goto toplevel directory
pushd "$(dirname "$0")/.."
HYRBIDSE_DIR=$(pwd)

# shellcheck disable=SC1091
source tools/init_env.profile.sh

if uname -a | grep -q Darwin; then
	# in case coreutils not install on mac
	alias nproc='sysctl -n hw.logicalcpu'
fi

mkdir -p build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DTESTING_ENABLE=ON
make zetasql_parser_test -j"$(nproc)"
make fe_slice_test -j"$(nproc)"
./src/parser/zetasql_parser_test
./src/base/fe_slice_test

popd
