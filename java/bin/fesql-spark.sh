#!/bin/bash

# fesql-spark.sh
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
DEFAULT_FESQL_SPARK_JAR=$(find ${CUR_DIR}/../java | grep fesql-spark-.*-with-dependencies.jar)
FESQL_SPARK_JAR=${FESQL_SPARK_JAR:=${DEFAULT_FESQL_SPARK_JAR}}
JVM_PROFILER_JAR=""
SPARK_JARS=""
USE_SPARK_SUBMIT=0
USE_SPARK_BENCH=0
APP_ARGS=""
SPARK_SUBMIT_ARGS=""

i=1;
while [[ ${i} -le $# ]]; do
    eval key=\$$i
    let i++
    eval value=\$$i
    case ${key} in
        -h|--help)       ;;
        --spark-submit)  USE_SPARK_SUBMIT=1;;
        --fesql-jar)     FESQL_SPARK_JAR=${value}; let i++;;
        -s|--sql)        APP_ARGS+=" ${key} ${value}"; let i++;;
        --spark-sql)     APP_ARGS+=" ${key}";;
        -s|--sql)        APP_ARGS+=" ${key} ${value}"; let i++;;
        -i|--input)      APP_ARGS+=" ${key} ${value}"; let i++;;
        -o|--output)     APP_ARGS+=" ${key} ${value}"; let i++;;
        -c)              APP_ARGS+=" ${key} ${value}"; let i++;;
        --conf)          APP_ARGS+=" ${key} ${value}";
                         SPARK_SUBMIT_ARGS+=" --conf ${value}"; let i++;;
        --master)        APP_ARGS+=" ${key} ${value}";
                         SPARK_SUBMIT_ARGS+=" --master ${value}"; let i++;;
        --name)          APP_ARGS+=" ${key} ${value}";
                         SPARK_SUBMIT_ARGS+=" --name ${value}"; let i++;;
        --jars)          SPARK_JARS+=",${value}"; let i++;;          
        -t|--time)       USE_SPARK_BENCH=1;;
        --jvm-profiler)  JVM_PROFILER_JAR=${value}; let i++;;
        ?) SPARK_SUBMIT_ARGS+=" ${key}";;
    esac
done;

echo "[Configurations]"
echo "- application jar: ${FESQL_SPARK_JAR}"
echo "- application arguments: ${APP_ARGS}"

if [[ ${FESQL_SPARK_JAR} == "" ]]; then
    echo "Application jar not set, exit application."
    exit 1
fi


if [[ ${USE_SPARK_SUBMIT} -eq 0 ]]; then
    JAVA_ARGS=""
    JAVA_CP="${FESQL_SPARK_JAR}"
    if [[ ${JVM_PROFILER_JAR} != "" ]]; then
        JAVA_CP+=":${JVM_PROFILER_JAR}"
        JAVA_ARGS+=" -javaagent:${JVM_PROFILER_JAR}=sampleInterval=50"
    fi

    if [[ ${USE_SPARK_BENCH} -eq 1 ]]; then
        time java -cp ${JAVA_CP} ${JAVA_ARGS} Main ${APP_ARGS}
    else
        java -cp ${JAVA_CP} ${JAVA_ARGS} Main ${APP_ARGS}
    fi
else
    if [[ ${SPARK_HOME} == "" ]]; then
        echo "SPARK_HOME not set, exit application."
        exit 1
    fi
    
    if [[ ${JVM_PROFILER_JAR} != "" ]]; then
        SPARK_SUBMIT_ARGS+=" --conf spark.executor.extraJavaOptions=-javaagent:${JVM_PROFILER_JAR}=sampleInterval=50"
        SPARK_SUBMIT_ARGS+=" --conf spark.driver.extraJavaOptions=-javaagent:${JVM_PROFILER_JAR}=sampleInterval=50"
        SPARK_JARS+=",${JVM_PROFILER_JAR}"
    fi
    
    if [[ ${SPARK_JARS} != "" ]]; then
        SPARK_SUBMIT_ARGS+=" --jars ${SPARK_JARS}" 
    fi

    echo "- SPARK_HOME variable: ${SPARK_HOME}"    
    echo "- spark-submit arguments: ${SPARK_SUBMIT_ARGS}"
    
    if [[ ${USE_SPARK_BENCH} -eq 1 ]]; then
        benchspark spark-submit ${SPARK_SUBMIT_ARGS} \
             --class Main ${FESQL_SPARK_JAR} ${APP_ARGS}
    else
        spark-submit ${SPARK_SUBMIT_ARGS} \
             --class Main ${FESQL_SPARK_JAR} ${APP_ARGS}
    fi
fi





