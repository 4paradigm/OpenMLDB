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

cd "$(dirname "$0")"
cd "$(git rev-parse --show-toplevel)"

pushd examples/toydb/onebox/
sh start_all.sh
popd

cd java
mvn scoverage:report
TEST_SUCCESS=$?
if [[ -x "$(command -v pages)" ]]; then
    rm -rf public
    mkdir public
    cp -r ./target/site/scoverage/* public/
    pages public false
fi
exit ${TEST_SUCCESS}
