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

echo "CICD environment tag: ${CICD_RUNNER_TAG}"

echo "Third party packages path: ${CICD_RUNNER_THIRDPARTY_PATH}"
if [[ "$OSTYPE" == "linux-gnu"* ]]
then
    ln -sf /depends/thirdparty thirdparty
    source /opt/rh/devtoolset-7/enable
    source /opt/rh/sclo-git25/enable
    [ -r /etc/profile.d/enable-thirdparty.sh ] && source /etc/profile.d/enable-thirdparty.sh
else
    source ~/.bash_profile
    ln -sf ${CICD_RUNNER_THIRDPARTY_PATH} thirdparty
fi
