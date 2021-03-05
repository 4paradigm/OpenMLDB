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

pip3 install pyyaml
python3 ${CUR_DIR}/udf_doxygen/export_udf_doc.py &>doxygen.log
if [[ "$?" -ne 0 ]]; then
	cat doxygen.log
	exit 1
fi

# CI_PROJECT_NAMESPAC=ai-native-db
# CI_PROJECT_NAME=fesql
# CI_COMMIT_REF_NAME=feat/ExportUDFDoc

HOST_PATH="https://nexus.4pd.io/repository/raw-hosted/${CI_PROJECT_NAMESPACE}/${CI_PROJECT_NAME}/${CI_COMMIT_REF_NAME}"
echo "Save udf pages to url: ${HOST_PATH}/udfdoc/udfs_8h.html"

HTML_FILES=$(find ${CUR_DIR}/udf_doxygen/html)
for path in ${HTML_FILES}; do
	if [[ -d "$path" ]]; then
		continue
	fi
	fname=$(python3 -c "import os; print(os.path.relpath(\"${path}\", \"${CUR_DIR}/udf_doxygen/html\"))")
	# echo "Upload $fname from $path"
    curl  --user 'deploy:GlW5SRo1TC3q' \
          --upload-file "$path" \
          ${HOST_PATH}/udfdoc/"$fname"
done
