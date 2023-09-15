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

import pytest
from diagnostic_tool.diagnose import parse_arg, main
from absl import flags
from .case_conf import OpenMLDB_ZK_CLUSTER
import os


def test_helpmsg():
    # parse_arg parses argv[1:], so add foo in the head
    with pytest.raises(SystemExit):
        parse_arg(["foo", "status", "-h"])
    with pytest.raises(SystemExit):
        parse_arg(["foo", "inspect", "-h"])
    with pytest.raises(SystemExit):
        parse_arg(["foo", "inspect", "online", "-h"])
    with pytest.raises(SystemExit):
        parse_arg(["foo", "test", "-h"])
    with pytest.raises(SystemExit):
        parse_arg(["foo", "static-check", "-h"])
    with pytest.raises(SystemExit):
        parse_arg(["foo", "--helpfull"])
    with pytest.raises(SystemExit):
        parse_arg(["foo", "rpc", "-h"])

def test_argparse():
    cluster_arg = f"--cluster={OpenMLDB_ZK_CLUSTER}"
    # -1 (warning),0 (info), 1 (debug), app default is -1->0
    # parse_arg parses argv[1:], so add foo in the head
    args = parse_arg(
        ["foo", "status", cluster_arg, "--diff", "--conf_file=hosts", "-v=1"]
    )
    assert flags.FLAGS.cluster == OpenMLDB_ZK_CLUSTER
    assert args.diff == True
    flags.FLAGS["verbosity"].unparse()

    args = parse_arg(["foo", "--sdk_log"])
    assert hasattr(args, "command"), "should print help"
    assert flags.FLAGS.sdk_log
    flags.FLAGS["sdk_log"].unparse()  # reset it, don't effect tests bellow

    # log setting by module, std logging name:level, but can't test now
    # parse_arg(['test', cluster_arg, '-v=-1', '--logger_levels=foo.bar:INFO'])
    # flags.FLAGS['logger_levels'].unparse()
    # flags.FLAGS['verbosity'].unparse()

    new_addr = "111.0.0.1:8181/openmldb"
    cluster_arg = f"--cluster={new_addr}"
    parse_arg(["foo", "inspect", "online", cluster_arg])
    assert flags.FLAGS.cluster == new_addr

    assert (
        not hasattr(args, "version")
        and not hasattr(args, "conf")
        and not hasattr(args, "log")
    )
    args = parse_arg(["foo", "static-check", "-VCL", "-v=0"])
    assert args.version and args.conf and args.log
    flags.FLAGS["verbosity"].unparse()


def test_cmd():
    # singleton connection
    cluster_arg = f"--cluster={OpenMLDB_ZK_CLUSTER}"
    args = parse_arg(
        [
            "foo",
            "status",
            cluster_arg,
        ]
    )
    main(args)
    # still connect to OpenMLDB_ZK_CLUSTER
    args = parse_arg("foo status --cluster=foo/bar".split(" "))
    main(args)


@pytest.mark.skip(reason="need to test in demo docker")
def test_cmd_static_check():
    # skip ssh
    args = parse_arg(f"foo static-check -f={os.path.dirname(__file__)}/hosts -C --local".split(" "))
    main(args)
