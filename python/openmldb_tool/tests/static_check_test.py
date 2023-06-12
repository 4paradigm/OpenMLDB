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

import logging
import os.path
import pytest

from diagnostic_tool.collector import Collector
from diagnostic_tool.conf_validator import ClusterConfValidator
from diagnostic_tool.dist_conf import read_conf
from diagnostic_tool.log_analyzer import LogAnalyzer
from absl import flags


logging.basicConfig(
    level=logging.DEBUG,
    format="{%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
)

current_path = os.path.dirname(__file__)

def mock_path(dist_conf):
    for _,v in dist_conf.server_info_map.items():
        for server in v:
            if server.path:
                server.path = current_path + '/sbin_test' + server.path

@pytest.mark.skip(reason="need to test in demo docker")
def test_in_demo_docker():
    flags.FLAGS['local'].parse('True') # only test local
    flags.FLAGS['default_dir'].parse('/work/openmldb') # if don't mock path, test the cluster in demo docker
    dist_conf = read_conf(current_path + "/hosts")
    local_collector = Collector(dist_conf)
    assert local_collector.pull_config_files("/tmp/conf_copy_dest")
    assert dist_conf.is_cluster()
    assert ClusterConfValidator(dist_conf, "/tmp/conf_copy_dest").validate()
    version_map = local_collector.collect_version()
    assert version_map
    print(version_map)
    # plz start cluster in demo docker first
    assert local_collector.pull_log_files("/tmp/log_copy_dest")
    LogAnalyzer(dist_conf, "/tmp/log_copy_dest").run()

    flags.FLAGS['local'].unparse()

# Remote test require ssh config, skip now. Only test local collector
def test_local_collector():
    flags.FLAGS['local'].parse('True') # only test local
    flags.FLAGS['default_dir'].parse('/work/openmldb') # if don't mock path, test the cluster in demo docker
    dist_conf = read_conf(current_path + "/hosts")
    mock_path(dist_conf) # test the cluster in sbin_test/
    local_collector = Collector(dist_conf)
    # no need to ping localhost when flags.FLAGS.local==True
    with pytest.raises(AssertionError):
        local_collector.ping_all()

    # no bin in tests/sbin_test/, so it's a empty map
    version_map = local_collector.collect_version()
    assert not version_map

    assert local_collector.pull_config_files("/tmp/conf_copy_dest")
    assert dist_conf.is_cluster()
    assert ClusterConfValidator(dist_conf, "/tmp/conf_copy_dest").validate()

    # all no logs in sbin_test/
    assert not local_collector.pull_log_files("/tmp/log_copy_dest")

    flags.FLAGS['local'].unparse()
