import os
import re

import pytest
from diagnostic_tool.parser import log_parser

err_log_list = os.listdir("off_err_logs")


@pytest.fixture(params=err_log_list)
def log_fixture(request):
    yield os.path.join("off_err_logs", request.param)


def test_pattern_logs(log_fixture):
    print("in", log_fixture)
    with open(log_fixture, "r") as f:
        log = f.read()
    err_lines = log_parser(log)
    print(*err_lines, sep="\n")
