# tools/autotest/auto_cases.py
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
import yaml
import logging
import uuid
import time
import subprocess
import multiprocessing
import traceback

from fesql_case import gen_single_window_test
from fesql_const import SQL_ENGINE_CASE_ERROR, SQL_ENGINE_COMPILE_ERROR, SQL_ENGINE_RUN_ERROR
from fesql_function import UDFPool
from fesql_param import parse_args

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s: %(message)s')

def worker_run(udf_pool, args, shared_states):
    # extract shared states
    total_cases, total_failures, history_udf_failures, history_udf_successes = shared_states

    while True:
        try:
            with total_cases.get_lock():
                total_cases.value += 1
            udf_pool.history_failures = history_udf_failures
            udf_pool.history_successes = history_udf_successes

            test_name = str(uuid.uuid1())
            case = gen_single_window_test(test_name, udf_pool, args)
            case_dir = os.path.join(args.log_dir, test_name)
            if not os.path.exists(case_dir):
                os.makedirs(case_dir)
            with open(os.path.join(case_dir, "case.yaml"), "w") as yaml_file:
                yaml_file.write(yaml.dump(case))

            test_bin = os.path.abspath(os.path.join(args.bin_path, "src/fesql_run_engine"))
            log_file = open(os.path.join(case_dir, "case.log"), "w")
            exitcode = subprocess.call([test_bin, "--yaml_path=case.yaml"],
                                       cwd=case_dir, stdout=log_file, stderr=log_file)
            log_file.close()
            if exitcode == 0:
                # logging.info("Success: " + test_name)
                subprocess.call(["rm", "-r", case_dir])
                with history_udf_successes.get_lock():
                    for udf in udf_pool.picked_functions:
                        udf_idx = udf.unique_id
                        history_udf_successes[udf_idx] += 1
                        if history_udf_failures[udf_idx] > 2 ** 30:
                            history_udf_successes[udf_idx] = 1
                udf_pool.picked_functions = []
                continue

            # increment failure count for sampled functions
            with history_udf_failures.get_lock():
                for udf in udf_pool.picked_functions:
                    udf_idx = udf.unique_id
                    history_udf_failures[udf_idx] += 1
                    if history_udf_failures[udf_idx] > 2 ** 30:
                        history_udf_failures[udf_idx] = 1
            with total_failures.get_lock():
                total_failures.value += 1
            udf_pool.picked_functions = []

            # error handling
            if exitcode == SQL_ENGINE_CASE_ERROR:
                logging.error("Invalid case in " + test_name)
            elif exitcode == SQL_ENGINE_COMPILE_ERROR:
                logging.error("SQL compile error in " + test_name)
            elif exitcode == SQL_ENGINE_RUN_ERROR:
                logging.error("Engine run error in " + test_name)
            elif exitcode >= 128:
                logging.error("Core (signal=%d) in " % (exitcode - 128) + test_name)
            else:
                logging.error("Unknown error in " + test_name)
            with open(os.path.join(case_dir, "exitcode"), "w") as f:
                f.write(str(exitcode))

        except Exception as e:
            logging.error(e)
            traceback.print_exc()


def run(args):
    # 加载yml文件 封装成UDFDesc 并进行分类 创建了 UDFPool
    udf_pool = UDFPool(args.udf_path, args)
    logging.info("Successfully load udf information from %s", args.udf_path)

    if not os.path.exists(args.log_dir):
        os.makedirs(args.log_dir)

    total_cases = multiprocessing.Value("i", 0)
    total_failures = multiprocessing.Value("i", 0)
    history_udf_failures = multiprocessing.Array("i", udf_pool.history_failures)
    history_udf_successes = multiprocessing.Array("i", udf_pool.history_successes)
    shared_states = [total_cases, total_failures,
                     history_udf_failures, history_udf_successes]

    workers = []
    for i in range(args.workers):
        worker_process = multiprocessing.Process(
            target=worker_run,
            args=[udf_pool, args, shared_states])
        worker_process.daemon = True
        workers.append(worker_process)
    for worker in workers:
        worker.start()

    while True:
        time.sleep(5)
        logging.info("Run %d cases, get %d failures" %
                     (total_cases.value, total_failures.value))
        if args.max_cases is not None and total_cases.value > args.max_cases:
            break
        if args.show_udf_summary:
            def top_udfs(arr):
                items = []
                arr = sorted([_ for _ in enumerate(arr)], key=lambda x: -x[1])
                for udf_id, cnt in arr:
                    if cnt <= 0:
                        continue
                    items.append(str(udf_pool.udf_by_unique_id[udf_id]) + ": " + str(cnt))
                return items
            logging.info("Frequent udf in success cases: [" +
                         ", ".join(top_udfs(history_udf_successes))) + "]"
            logging.info("Frequent udf in failure cases: [" +
                         ", ".join(top_udfs(history_udf_failures))) + "]"
    logging.info("Wait workers to exit...")

def main(args):
    run(args)

if __name__ == "__main__":
    main(parse_args())
