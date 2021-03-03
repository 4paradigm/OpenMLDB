# tools/autotest/gen_case_yaml_main.py
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

import os
import time
import uuid
import yaml

from fesql_case import gen_single_window_test
from fesql_function import UDFPool
from fesql_param import parse_args

from fesql_case import gen_window_union_test
from fesql_param import sample_integer_config

from fesql_case import gen_window_lastjoin_test, gen_window_subselect_test

gen_sql = {
    0: gen_single_window_test,
    1: gen_window_union_test,
    2: gen_window_lastjoin_test,
    3: gen_window_subselect_test,
}

def gen_case_yaml(case_dir=None):
    args = parse_args()
    udf_pool = UDFPool(args.udf_path, args)
    begin = time.time()
    case_num = args.yaml_count
    if case_dir == None:
        case_dir = args.log_dir
    if not os.path.exists(case_dir):
        os.makedirs(case_dir)
    for i in range(case_num):
        sql_type = sample_integer_config(args.sql_type)
        test_name = str(uuid.uuid1())
        case = gen_sql[sql_type](test_name, udf_pool, args)
        yamlName = "auto_gen_case_"+str(i)+".yaml"
        with open(os.path.join(case_dir, yamlName), "w") as yaml_file:
            yaml_file.write(yaml.dump(case))
    end = time.time()
    print("use time:"+str(end-begin))

if __name__ == "__main__":
    '''
    生成yaml的入口
    '''
    currentPath = os.getcwd()
    index = currentPath.rfind('fesql')
    if index == -1:
        prePath = currentPath+"/"
    else:
        prePath = currentPath[0:index]
    print("prePath:"+prePath)
    casePath = prePath+"fesql/cases/auto_gen_cases"
    print("casePath:"+casePath)
    gen_case_yaml(casePath)
