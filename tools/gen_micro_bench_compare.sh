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

curl -o baseline_report.txt \
    https://nexus.4pd.io/repository/raw-hosted/ai-native-db/fesql/develop/micro_bench_report.txt

python tools/benchmark_report/compare_report.py baseline_report.txt micro_bench_report.txt 

echo "Baseline benchmark comparation url: https://nexus.4pd.io/repository/raw-hosted/${CI_PROJECT_NAMESPACE}/${CI_PROJECT_NAME}/${CI_COMMIT_REF_NAME}/benchmark_compare.html"

curl  --user 'deploy:GlW5SRo1TC3q' --upload-file micro_bench_report.txt  "https://nexus.4pd.io/repository/raw-hosted/${CI_PROJECT_NAMESPACE}/${CI_PROJECT_NAME}/${CI_COMMIT_REF_NAME}/micro_bench_report.txt"
curl  --user 'deploy:GlW5SRo1TC3q' --upload-file benchmark_compare.html  "https://nexus.4pd.io/repository/raw-hosted/${CI_PROJECT_NAMESPACE}/${CI_PROJECT_NAME}/${CI_COMMIT_REF_NAME}/benchmark_compare.html"
