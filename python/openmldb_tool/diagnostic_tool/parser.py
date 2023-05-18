import re
import yaml

from diagnostic_tool.connector import Connector
from diagnostic_tool.server_checker import StatusChecker


class LogParser:
    def __init__(self, log_conf_file: str) -> None:
        with open(log_conf_file) as f:
            self.conf = yaml.safe_load(f)
        self.errs = self.conf["errors"]

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
            elif skip_flag:
                print("...")
                skip_flag = False
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


class ErrSolution:
    def __init__(self, err) -> None:
        self.desc = err["description"]
        self.solution = err["solution"]
        self.result = ""

    def __call__(self, *args, **kwargs):
        exec(f"self.{self.solution}()")
        return self.result

    def zk_conn_err(self):
        self.result += "\n" + self.desc
        self.result += "\nChecking zk connection..."
        conn = Connector()
        checker = StatusChecker(conn)
        assert checker._get_components(show=False)
        self.result += "\nSuccessfully checked zk connection, it may be caused by `Too many connections` in zk server, please check zk server log."
