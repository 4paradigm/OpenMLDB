#!/bin/bash

CUR_DIR=$(cd $(dirname $0); pwd)

python ${CUR_DIR}/udf_doxygen/export_udf_doc.py &>doxygen.log
if [[ "$?" -ne 0 ]]; then
	cat doxygen.log
fi