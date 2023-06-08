import re


def log_parser(log):
    log_lines = log.split("\n")
    error_patterns = [
        re.compile(r"at com.*openmldb"),
        re.compile(r"At .*OpenMLDB"),
        re.compile(r"Caused by"),
        re.compile(r"java.*Exception"),
        re.compile(r"Exception in"),
        re.compile(r"ERROR"),
    ]

    error_messages = []
    skip_flag = 0

    for line in log_lines:
        for pattern in error_patterns:
            match = pattern.search(line)
            if match:
                error_messages.append(line)
                skip_flag = 1
                break
        else:
            if skip_flag:
                error_messages.append("...")
                skip_flag = 0

    return error_messages
