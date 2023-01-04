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

from diagnostic_tool.dist_conf import DistConfReader
import os


def test_read():
    current_path = os.path.dirname(__file__)
    dist = DistConfReader(current_path + "/cluster_dist.yml").conf()
    assert dist.mode == "cluster"
    assert len(dist.server_info_map.map["nameserver"]) == 1
    assert len(dist.server_info_map.map["tablet"]) == 2
