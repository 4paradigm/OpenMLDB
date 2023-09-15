import os
from absl import flags

import pytest
from diagnostic_tool.parser import LogParser
from .case_conf import OpenMLDB_ZK_CLUSTER

logs_path = os.path.join(os.path.dirname(__file__), "off_err_logs")
err_log_list = [os.path.join(logs_path, err_log) for err_log in os.listdir(logs_path)]


@pytest.mark.parametrize("err_log", err_log_list)
def test_pattern_logs(err_log):
    flags.FLAGS['cluster'].parse(OpenMLDB_ZK_CLUSTER)
    flags.FLAGS['sdk_log'].parse(False)
    print("in", err_log)
    with open(err_log, "r") as f:
        log = f.read()
    parser = LogParser()
    parser.update_conf_file("https://openmldb.ai/download/diag/common_err.yml")
    parser.parse_log(log)
