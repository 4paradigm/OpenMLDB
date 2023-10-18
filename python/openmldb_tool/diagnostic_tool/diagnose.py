#!/usr/bin/env python3
# -*- coding: utf-8 -*-
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

import argparse
import json
import os
import textwrap
import time

from diagnostic_tool.connector import Connector
from diagnostic_tool.dist_conf import read_conf
from diagnostic_tool.conf_validator import (
    DistConfValidator,
    ClusterConfValidator,
)
from diagnostic_tool.log_analyzer import LogAnalyzer
from diagnostic_tool.collector import Collector
import diagnostic_tool.server_checker as checker
from diagnostic_tool.table_checker import TableChecker
from diagnostic_tool.parser import LogParser
from .inspect import server_ins, table_ins, partition_ins, ops_ins, inspect_hint
from .rpc import RPC

from absl import app
from absl import flags
from absl.flags import argparse_flags
from absl import logging  # --verbosity --log_dir

# only some sub cmd needs dist file TODO(hw): better to move then to other py file, to avoid -h show them
flags.DEFINE_string(
    "conf_file",
    "",
    "Cluster config file, supports two styles: yaml and hosts.",
    short_name="f",
)
flags.DEFINE_bool(
    "local",
    False,
    "If set, all server in config file will be treated as local server, skip ssh.",
)
flags.DEFINE_string(
    "db",
    "",
    "Specify databases to diagnose, split by ','. Only used in inspect online.",
)

flags.DEFINE_string("collect_dir", "/tmp/diag_collect", "...")


def check_version(version_map: dict):
    # cluster must have nameserver, so we use nameserver version to be the right version
    version = version_map["nameserver"][0][1]
    flag = True
    for role, servers in version_map.items():
        for endpoint, cur_version in servers:
            if cur_version != version:
                logging.warning(
                    f"version mismatch. {role} {endpoint} version {cur_version} != {version}"
                )
                flag = False
    return version, flag


def status(args):
    """use OpenMLDB Python SDK to connect OpenMLDB"""
    connect = Connector()
    status_checker = checker.StatusChecker(connect)
    if not status_checker.check_components():
        print("some components is offline")

    # --diff with dist conf file, conf_file is required
    if args.diff:
        assert flags.FLAGS.conf_file, "need --conf_file/-f"
        print(
            "only check components in conf file, if cluster has more components, ignore them"
        )
        dist_conf = read_conf(flags.FLAGS.conf_file)
        assert status_checker.check_startup(
            dist_conf
        ), f"not all components in conf file are online, check the previous output"
        print(f"all components in conf file are online")

    if args.conn:
        status_checker.check_connection()


def inspect(args):
    # report all
    # 1. server level
    connect = Connector()
    status_checker = checker.StatusChecker(connect)
    server_map = status_checker._get_components()
    offlines = server_ins(server_map)

    # 2. table level
    # we use `show table status` instead of partition inspection, cuz it's simple and quick
    warn_tables = table_ins(connect)

    # if has unhealthy tables, do partition and ops check, otherwise skip
    hints = []
    if warn_tables:
        # 3. ns ops
        related_ops = ops_ins(connect)
        # 4. partition level and get some hint about table
        hints = partition_ins(server_map, related_ops)

    # 5. hint
    # let user know what to do
    # 1) start offline servers
    # 2) let user know the warning table is fatal or not, related ops, warn if offset is too large
    # 3) if table not healthy and no related ops, use recoverdata
    inspect_hint(offlines, hints)


def insepct_online(args):
    """inspect online"""
    connect = Connector()
    # scan all db include system db
    fails = table_ins(connect)
    for t in fails:
        if t[13]:
            print(f"unhealthy table {t[2]}.{t[1]}:\n {t[:13]}")
            # sqlalchemy truncated ref https://github.com/sqlalchemy/sqlalchemy/commit/591e0cf08a798fb16e0ee9b56df5c3141aa48959
            # so we print warnings alone
            print(f"full warnings:\n{t[13]}")
    # if has fails, summary will print in table_ins
    if not fails:
        print(f"all tables are healthy")

    if getattr(args, "dist", False):
        table_checker = TableChecker(connect)
        dbs = flags.FLAGS.db
        db_list = dbs.split(",") if dbs else None
        table_checker.check_distribution(dbs=db_list)


def inspect_offline(args):
    """scan jobs status, show job log if failed"""
    final_failed = ["failed", "killed", "lost"]
    total, num, jobs = _get_jobs(final_failed)
    # TODO some failed jobs are known, what if we want skip them?
    print(f"inspect {total} offline jobs")
    if num:
        failed_jobs_str = "\n".join(jobs)
        raise AssertionError(
            f"{num} offline final jobs are failed\nfailed jobs:\n{failed_jobs_str}"
        )
    print("all offline final jobs are finished")


def _get_jobs(states=None):
    assert checker.StatusChecker(Connector()).offline_support()
    conn = Connector()
    jobs = conn.execfetch("SHOW JOBS")
    total_num = len(jobs)
    # jobs sorted by id
    jobs.sort(key=lambda x: x[0])
    show_jobs = [
        _format_job_row(row) for row in jobs if not states or row[2].lower() in states
    ]
    return total_num, len(show_jobs), show_jobs


def _format_job_row(row):
    row = list(row)
    row[3] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(row[3] / 1000))
    row[4] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(row[4] / 1000))
    return " ".join(map(str, row))


def inspect_job(args):
    if not args.id:
        states = args.state.split(",") if args.state != "all" else None
        total, num, jobs = _get_jobs(states)
        print(f"inspect {total} offline jobs")
        if args.state != "all":
            print(f"{num} {args.state} jobs")
        print(*jobs, sep="\n")
        return
    conn = Connector()
    std_output = conn.execfetch(f"SHOW JOBLOG {args.id}")
    assert len(std_output) == 1 and len(std_output[0]) == 1
    detailed_log = std_output[0][0]
    if args.detail:
        print(detailed_log)
    else:
        parser = LogParser()
        if args.conf_update or not os.path.exists(parser.conf_file):
            parser.update_conf_file(args.conf_url)
        parser.parse_log(detailed_log)


def test_sql(args):
    conn = Connector()
    status_checker = checker.StatusChecker(conn)
    if not status_checker.check_components():
        logging.warning("some server is unalive, be careful")
    tester = checker.SQLTester(conn)
    tester.setup()
    print("test online")
    tester.online()
    if status_checker.offline_support():
        print("test offline")
        tester.offline()
    else:
        print("no taskmanager, can't test offline")
    tester.teardown()
    print("all test passed")


def static_check(args):
    assert flags.FLAGS.conf_file, "static check needs dist conf file"
    if not (args.version or args.conf or args.log):
        print("at least one arg to check, check `openmldb_tool static-check -h`")
        return
    dist_conf = read_conf(flags.FLAGS.conf_file)
    # the deploy path of servers may be flags.default_dir, we won't check if it's valid here.
    assert DistConfValidator(dist_conf).validate(), "conf file is invalid"
    collector = Collector(dist_conf)
    if args.version:
        versions = collector.collect_version()
        print(f"version:\n{versions}")  # TODO pretty print
        version, ok = check_version(versions)
        assert ok, f"all servers version should be {version}"
        print(f"version check passed, all {version}")
    if args.conf:
        collector.pull_config_files(flags.FLAGS.collect_dir)
        # config validate, read flags.FLAGS.collect_dir/<server-name>/conf
        if dist_conf.is_cluster():
            assert ClusterConfValidator(dist_conf, flags.FLAGS.collect_dir).validate()
        else:
            assert False, "standalone unsupported"
    if args.log:
        collector.pull_log_files(flags.FLAGS.collect_dir)
        # log check, read flags.FLAGS.collect_dir/logs
        # glog parse & java log
        LogAnalyzer(dist_conf, flags.FLAGS.collect_dir).run()


def rpc(args):
    connect = Connector()
    status_checker = checker.StatusChecker(connect)

    host = args.host
    if not host:
        status_checker.check_components()
        print(
            """choose one host to connect, e.g. "openmldb_tool rpc ns".        
        ns: nameserver(master only, no need to choose)
        tablet:you can get from component table, e.g. the first tablet in table is tablet1
        tm: taskmanager"""
        )
        return

    # use status connction to get version
    conns_with_version = {
        endpoint: version
        for endpoint, version, _, _ in status_checker.check_connection()
    }
    _, endpoint, _ = RPC.get_endpoint_service(host)
    proto_version = conns_with_version[endpoint]
    print(f"server proto version is {proto_version}")

    operation = args.operation
    field = json.loads(args.field)
    rpc_service = RPC(host)
    if args.hint:
        pb2_dir = flags.FLAGS.pbdir
        print(f"hint use pb2 files from {pb2_dir}")
        # check about rpc depends proto compiled dir
        if (
            not os.path.isdir(pb2_dir)
            or len([pb for pb in os.listdir(pb2_dir) if pb.endswith("_pb2.py")]) < 8
        ):
            print(f"{pb2_dir} is broken, mkdir and download")
            os.system(f"mkdir -p {pb2_dir}")
            import tarfile
            import requests

            # pb2.tar has no dir, extract to pb2_dir
            url = "https://openmldb.ai/download/diag/pb2.tgz"
            r = requests.get(url)
            with open(f"{pb2_dir}/pb2.tgz", "wb") as f:
                f.write(r.content)

            with tarfile.open(f"{pb2_dir}/pb2.tgz", "r:gz") as tar:
                tar.extractall(pb2_dir)
        rpc_service.hint(args.operation)
        return
    if not operation:
        print(
            "choose one operation, e.g. `openmldb_tool rpc ns ShowTable`, --hint for methods list or one method help"
        )
        return
    rpc_service(operation, field)


def parse_arg(argv):
    """parser definition, absl.flags + argparse"""
    parser = argparse_flags.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter
    )
    # use args.header returned by parser.parse_args
    subparsers = parser.add_subparsers(help="OpenMLDB Tool")

    # sub status
    status_parser = subparsers.add_parser(
        "status", help="check the OpenMLDB server status"
    )
    status_parser.add_argument(
        "--diff",
        action="store_true",
        help="check if all endpoints in conf are in cluster. If set, need to set `--conf_file`",
    )  # TODO action support in all python 3.x?
    status_parser.add_argument(
        "--conn",
        action="store_true",
        help="check network connection of all servers",
    )
    status_parser.set_defaults(command=status)

    # sub inspect
    inspect_parser = subparsers.add_parser(
        "inspect",
        help="Get full inspect report, --nocolor for batch mode, --table_width for partition tables display",
    )
    inspect_parser.set_defaults(command=inspect)

    inspect_sub = inspect_parser.add_subparsers()
    # inspect online
    online = inspect_sub.add_parser("online", help="only inspect online table.")
    online.set_defaults(command=insepct_online)
    online.add_argument(
        "--dist", action="store_true", help="Inspect online distribution."
    )
    # inspect offline
    offline = inspect_sub.add_parser(
        "offline", help="only inspect offline jobs, show failed jobs."
    )
    offline.set_defaults(command=inspect_offline)
    # inspect job
    ins_job = inspect_sub.add_parser(
        "job", help="show jobs by state, show joblog or parse joblog by id."
    )
    ins_job.set_defaults(command=inspect_job)
    ins_job.add_argument(
        "--state", default="all", help="Specify which state offline jobs, split by ','"
    )
    ins_job.add_argument("--id", help="inspect joblog by id")
    ins_job.add_argument(
        "--detail",
        action="store_true",
        help="show detailed joblog information, use with `--id`",
    )
    ins_job.add_argument(
        "--conf-url",
        default="https://raw.githubusercontent.com/4paradigm/OpenMLDB/main/python/openmldb_tool/diagnostic_tool/common_err.yml",
        help="url used to update the log parser configuration. If downloading is slow, you can try mirror source 'https://openmldb.ai/download/diag/common_err.yml'",
    )
    ins_job.add_argument(
        "--conf-update", action="store_true", help="update the log parser configuration"
    )

    # sub test
    test_parser = subparsers.add_parser(
        "test",
        help="Do simple create&insert&select test in online, select in offline(if taskmanager exists)",
    )
    test_parser.set_defaults(command=test_sql)

    # sub static-check
    static_check_parser = subparsers.add_parser(
        "static-check",
        help=textwrap.dedent(
            """ \
        Static check on remote host, version/conf/log, -h to show the arguments, --conf_file is required.
        Use -VCL to check all.
        You can check version or config before cluster running.
        If servers are remote, need Passwordless SSH Login.
        """
        ),
    )
    static_check_parser.add_argument(
        "--version", "-V", action="store_true", help="check version"
    )
    static_check_parser.add_argument(
        "--conf", "-C", action="store_true", help="check conf"
    )
    static_check_parser.add_argument(
        "--log", "-L", action="store_true", help="check log"
    )
    static_check_parser.set_defaults(command=static_check)

    # sub rpc
    rpc_parser = subparsers.add_parser(
        "rpc",
        help="user-friendly rpc tool",
    )
    rpc_parser.add_argument(
        "host",
        nargs="?",
        help=textwrap.dedent(
            """ \
        host name, if no value, print the component table. 
        ns: nameserver(master only, no need to choose)
        tablet:you can get from component table, e.g. the first tablet in table is tablet1
        tm: taskmanager
        """
        ),
    )
    rpc_parser.add_argument(
        "operation",
        nargs="?",
        default="",
    )
    rpc_parser.add_argument(
        "--field",
        default="{}",
        help='json format, e.g. \'{"db":"db1","table":"t1"}\', default is \'{}\'',
    )
    rpc_parser.add_argument(
        "--hint",
        action="store_true",
        help="print rpc hint for current operation(rpc method), if no operation, print all possible rpc methods",
    )
    rpc_parser.set_defaults(command=rpc)

    def help(args):
        parser.print_help()

    parser.set_defaults(command=help)

    args = parser.parse_args(argv[1:])
    tool_flags = {
        k: [flag.serialize() for flag in v]
        for k, v in flags.FLAGS.flags_by_module_dict().items()
        if "diagnostic_tool" in k
    }
    logging.debug(f"args:{args}, flags: {tool_flags}")

    return args


def main(args):
    # TODO: adjust args, e.g. if conf_file, we can get zk addr from conf file, no need to --cluster
    # run the command
    print(f"diagnosing cluster {Connector().address()}")
    args.command(args)


def run():
    app.run(main, flags_parser=parse_arg)


if __name__ == "__main__":
    app.run(main, flags_parser=parse_arg)
