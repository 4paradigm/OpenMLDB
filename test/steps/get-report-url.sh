#!/usr/bin/env bash
job_name=$1
build_name=$2
jenkins_job_url="http://auto.4paradigm.com/job/$job_name"
build_time_graph="$jenkins_job_url/buildTimeGraph/map"
build_history=$(curl -s "$build_time_graph")

for retry in $(seq 1 60)
do
  if [[ $build_history =~ $build_name ]]
  then
    build_number=$(curl -s "$build_time_graph" |grep "$build_name" | sed -r 's/.*ref\=\"//g'|grep -Eo '[0-9]+')
    echo "Test report visit jenkins url -> $jenkins_job_url/$build_number/allure/"
    exit 0
  fi
  sleep 10s
  build_history=$(curl -s" $build_time_graph")
  echo "Waiting for report generation $retry time ..."
done
echo "No report!"