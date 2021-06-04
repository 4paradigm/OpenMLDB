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

# goto the toplevel directory
pushd "$(dirname "$0")/.."

echo "CICD environment tag: ${CICD_RUNNER_TAG}"

echo "Third party packages path: ${CICD_RUNNER_THIRDPARTY_PATH}"
if [[ "$OSTYPE" == "linux-gnu"* ]]
then
    # unpack thirdparty first time
    pushd /depends
    if [[ ! -d thirdparty && -r thirdparty.tar.gz ]]; then
        mkdir -p thirdparty
        tar xzf thirdparty.tar.gz -C thirdparty --strip-components=1
        curl -SL -o libzetasql.tar.gz https://github.com/jingchen2222/zetasql/releases/download/v0.2.0-bata5/libzetasql-0.2.0-bata5-linux-x86_64.tar.gz
        tar xzf libzetasql.tar.gz -C  thirdparty --strip-components 1
        rm libzetasql.tar.gz

    fi
    popd

    ln -sf /depends/thirdparty thirdparty

    if [ -r /etc/profile.d/enable-thirdparty.sh ]; then
        # shellcheck disable=SC1091
        source /etc/profile.d/enable-thirdparty.sh
    fi
else
    # shellcheck disable=SC1090
    source ~/.bash_profile
    ln -sf "${CICD_RUNNER_THIRDPARTY_PATH}" thirdparty
fi
popd
