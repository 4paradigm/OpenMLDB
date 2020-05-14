#!/bin/bash

CUR_DIR=$(cd $(dirname $0); pwd)
DEFAULT_FESQL_SPARK_JAR=$(find ${CUR_DIR}/../java | grep fesql-spark-.*-with-dependencies.jar)
FESQL_SPARK_JAR=${FESQL_SPARK_JAR:=${DEFAULT_FESQL_SPARK_JAR}}
USE_SPARK_SUBMIT=0
APP_ARGS=""
SPARK_SUBMIT_ARGS=""

i=0;
while [[ ${i} -lt $# ]]; do
    eval key=\$$i
    let i++
    eval value=\$$i

    case ${key} in
        --spark-submit) USE_SPARK_SUBMIT=1;;
        --fesql-jar) FESQL_SPARK_JAR=${value}; let i++;;
        --spark-sql) APP_ARGS+=" ${key}";;
        -s|--sql) APP_ARGS+=" ${key} ${value}"; let i++;;
        -o|--output) APP_ARGS+=" ${key} ${value}"; let i++;;
        -c) let i++; APP_ARGS+=" ${key} ${value}"; let i++;;
        --conf) APP_ARGS+=" ${key} ${value}"; SPARK_SUBMIT_ARGS+=" --conf ${value}"; let i++;;
        --master) APP_ARGS+=" ${key} ${value}"; SPARK_SUBMIT_ARGS+=" --master ${value}"; let i++;;
        --name) APP_ARGS+=" ${key} ${value}"; SPARK_SUBMIT_ARGS+=" --name ${value}"; let i++;;
        ?) SPARK_SUBMIT_ARGS+=" ${key}";;
    esac
done;


# jar path
echo "Find fesql-spark jar package: ${FESQL_SPARK_JAR}"
echo "Application arguments: ${APP_ARGS}"

# use spark-submit
if [[ ${USE_SPARK_SUBMIT} -eq 0 ]]; then
    java -cp ${FESQL_SPARK_JAR} Main $@
else
    echo "Spark-submit arguments: ${SPARK_SUBMIT_ARGS}"
    spark-submit ${SPARK_SUBMIT_ARGS} ${FESQL_SPARK_JAR} ${APP_ARGS}
fi





