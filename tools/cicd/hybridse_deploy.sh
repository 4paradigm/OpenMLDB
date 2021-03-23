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

CUR_DIR=$(cd $(dirname $0); pwd)

# CI_PROJECT_NAMESPAC=ai-native-db
# CI_PROJECT_NAME=hybridse

HOST_PATH="https://nexus.4pd.io/repository/raw-hosted/${CI_PROJECT_NAMESPACE}/${CI_PROJECT_NAME}/${CI_COMMIT_REF_NAME}"
FILE_NAME=hybridse-release-0.1.0.tar.gz
echo "Upload hybridse to url: ${HOST_PATH}/hybridse/${FILE_NAME}"
curl  --user 'deploy:GlW5SRo1TC3q' \
      --upload-file ${FILE_NAME} \
      ${HOST_PATH}/hybridse/"${FILE_NAME}"
