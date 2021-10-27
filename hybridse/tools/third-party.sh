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

# setup third-party on hybridse
set -eE

pushd "$(dirname "$0")/.." # $ROOT/hybridse/
HYBRIDSE_ROOT=$(pwd)

# install cmake managed dependencies
cmake -S "$HYBRIDSE_ROOT/../third-party" -B "$HYBRIDSE_ROOT/.deps"
cmake --build "$HYBRIDSE_ROOT/.deps"

# install dependencies from hybridsql-asserts
# TODO: managed all dependencies directly from cmake
HYBRIDSE_THIRDPARTY="$HYBRIDSE_ROOT/.deps/usr"
../steps/setup_thirdparty.sh -p "${HYBRIDSE_THIRDPARTY}" -z 0 -f

popd
