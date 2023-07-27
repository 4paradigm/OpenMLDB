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
import re
import requests
import warnings
import yaml

from diagnostic_tool.connector import Connector
from diagnostic_tool.server_checker import StatusChecker

CONF_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "common_err.yml")


class LogParser:
    def __init__(self, log_conf_file=CONF_FILE) -> None:
        self.conf_file = log_conf_file
        self._load_conf()

    def _load_conf(self):
        self.errs = yaml.safe_load(open(self.conf_file))["errors"]

    def parse_log(self, log: str):
        log_rows = log.split("\n")
        # solution results
        solution_results = []
        # skip irrelevant rows
        skip_flag = False
        for row in log_rows:
            result = self._parse_row(row)
            if result:
                if result != "null":
                    solution_results.append(result)
                skip_flag = True
                continue
            # print "..." if some lines are skipped
            else:
                if skip_flag:
                    print("...")
                    skip_flag = False
        if solution_results:
            print("Solutions".center(50, "="))
            print(*solution_results, sep="\n")

    def _parse_row(self, row):
        for name, value in self.errs.items():
            for pattern in value['patterns']:
                if re.search(pattern, row):
                    print(row)
                    if "solution" in self.errs[name]:
                        solution = ErrSolution(self.errs[name])
                        result = solution()
                        return result
                    return "null"

    def update_conf_file(self, log_conf_url):
        response = requests.get(log_conf_url)
        if response.status_code == 200:
            with open(self.conf_file, "w") as f:
                f.write(response.text)
        else:
            warnings.warn("log parser configuration update failed")
        self._load_conf()


class ErrSolution:
    def __init__(self, err) -> None:
        self.desc = err["description"]
        self.solution = err["solution"]
        self.result = ""

    def __call__(self, *args, **kwargs):
        getattr(self, self.solution)()
        return self.result

    def zk_conn_err(self):
        self.result += "\n" + self.desc
        self.result += "\nChecking zk connection..."
        conn = Connector()
        checker = StatusChecker(conn)
        assert checker._get_components(show=False), "Failed to connect to zk"
        self.result += "\nSuccessfully checked zk connection. It may be caused by `Too many connections` in zk server. Please check zk server log."
