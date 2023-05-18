import os
import re
from absl import flags

import pytest
from diagnostic_tool.parser import LogParser

err_log_list = [os.path.join("off_err_logs", err_log) for err_log in os.listdir("off_err_logs")]


@pytest.mark.parametrize("err_log", err_log_list)
def test_pattern_logs(err_log):
    flags.FLAGS['cluster'].parse('127.0.0.1:2181/openmldb')
    flags.FLAGS['sdk_log'].parse(False)
    print("in", err_log)
    with open(err_log, "r") as f:
        log = f.read()
    parser = LogParser("common_err.yml")
    parser.parse_log(log)
