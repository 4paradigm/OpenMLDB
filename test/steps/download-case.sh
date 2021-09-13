#!/usr/bin/env bash

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

CASE_BRANCH=$1
if [[ "${CASE_BRANCH}" == "" ]]; then
    CASE_BRANCH="main"
fi
ROOT_DIR=$(pwd)
sh "${ROOT_DIR}"/stps/retry-command.sh "git clone -b ${CASE_BRANCH} https://github.com/4paradigm/OpenMLDB.git"