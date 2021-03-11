#!/usr/bin/env bash
job_name=$1
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

build_name=$2
jenkins_job_url="http://auto.4paradigm.com/job/$job_name"
build_time_graph="$jenkins_job_url/buildTimeGraph/map"
build_history=$(curl -s $build_time_graph)

for retry in $(seq 1 60)
do
  if [[ $build_history =~ $build_name ]]
  then
    build_number=$(curl -s $build_time_graph |grep $build_name | sed -r 's/.*ref\=\"//g'|grep -Eo '[0-9]+')
    echo "Test report visit jenkins url -> $jenkins_job_url/$build_number/allure/"
    exit 0
  fi
  sleep 10s
  build_history=$(curl -s $build_time_graph)
  echo "Waiting for report generation $retry time ..."
done
echo "No report!"
