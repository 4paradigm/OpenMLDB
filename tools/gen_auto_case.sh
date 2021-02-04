#!/usr/bin/env bash

python3 tools/autotest/gen_case_yaml_main.py  \
    --udf_path=tools/autotest/udf_defs.yaml \
    --yaml_count=5 \
    --gen_time_mode=auto \
    --window_num=1 \
    --window_type=1