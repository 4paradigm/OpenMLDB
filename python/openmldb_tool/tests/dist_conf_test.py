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

from diagnostic_tool.dist_conf import YamlConfReader, HostsConfReader, read_conf
import os
from absl import flags

def yaml_asserts(dist):
    assert dist.mode == "cluster"
    assert len(dist.server_info_map.map["nameserver"]) == 1
    assert len(dist.server_info_map.map["tablet"]) == 2
    assert dist.server_info_map.map["nameserver"][0].endpoint == "127.0.0.1:6527"
    assert dist.server_info_map.map["nameserver"][0].path == "/work/ns1"

def test_read_yaml():
    current_path = os.path.dirname(__file__)
    dist = YamlConfReader(current_path + "/cluster_dist.yml").conf()
    yaml_asserts(dist)

def hosts_asssert(dist):
    assert dist.mode == "cluster"
    assert len(dist.server_info_map.map["nameserver"]) == 1
    assert len(dist.server_info_map.map["tablet"]) == 2
    assert dist.server_info_map.map["nameserver"][0].endpoint == "localhost:7527"
    assert dist.server_info_map.map["nameserver"][0].path == flags.FLAGS.default_dir
    d = dist.count_dict()
    assert d['zookeeper'] == 1

def test_read_hosts():
    current_path = os.path.dirname(__file__)
    dist = HostsConfReader(current_path + "/hosts").conf()
    hosts_asssert(dist)


def test_auto_read():
    current_path = os.path.dirname(__file__)
    # read in yaml style failed, then read in hosts style
    dist = read_conf(current_path + "/hosts")
    hosts_asssert(dist)
    dist = read_conf(current_path + "/cluster_dist.yml")
    yaml_asserts(dist)
