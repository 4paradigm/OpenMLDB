#!/bin/bash

CUR_DIR=$(cd $(dirname $0); pwd)

python ${CUR_DIR}/udf_doxygen/export_udf_doc.py &>doxygen.log
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
	fname=$(realpath --relative-to=${CUR_DIR}/udf_doxygen/html $path)
	# echo "Upload $fname from $path"
    curl  --user 'deploy:GlW5SRo1TC3q' \
          --upload-file "$path" \
          ${HOST_PATH}/udfdoc/"$fname"
done
