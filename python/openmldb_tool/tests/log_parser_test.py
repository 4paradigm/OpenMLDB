import os
import re

import pytest
from diagnostic_tool.parser import LogParser

err_log_list = [os.path.join("off_err_logs", err_log) for err_log in os.listdir("off_err_logs")]


@pytest.mark.parametrize("err_log", err_log_list)
def test_pattern_logs(err_log):
    print("in", err_log)
    with open(err_log, "r") as f:
        log = f.read()
    parser = LogParser("common_err.yml")
    parser.parse_log(log)
