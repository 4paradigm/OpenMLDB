#!/bin/bash

curl -o baseline_report.txt \
    https://nexus.4pd.io/repository/raw-hosted/ai-native-db/fesql/develop/micro_bench_report.txt

python tools/benchmark_report/compare_report.py baseline_report.txt micro_bench_report.txt 

echo "Baseline benchmark comparation url: https://nexus.4pd.io/repository/raw-hosted/${CI_PROJECT_NAMESPACE}/${CI_PROJECT_NAME}/${CI_COMMIT_REF_NAME}/benchmark_compare.html"

curl  --user 'deploy:GlW5SRo1TC3q' --upload-file micro_bench_report.txt  https://nexus.4pd.io/repository/raw-hosted/${CI_PROJECT_NAMESPACE}/${CI_PROJECT_NAME}/${CI_COMMIT_REF_NAME}/micro_bench_report.txt
curl  --user 'deploy:GlW5SRo1TC3q' --upload-file benchmark_compare.html  https://nexus.4pd.io/repository/raw-hosted/${CI_PROJECT_NAMESPACE}/${CI_PROJECT_NAME}/${CI_COMMIT_REF_NAME}/benchmark_compare.html
