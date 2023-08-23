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

import random
from tool import Status

class Util:
    def gen_distribution(tablets : list, replica_num: int = 3, partition_num : int = 8) -> (Status, str):
        if replica_num < 1 or len(tablets) < replica_num:
            return Status(-1, "invalid args")
        distribution = "["
        for i in range(partition_num):
            if i > 0:
                distribution += ","
            endpoints = random.sample(tablets, replica_num)
            distribution += f"(\'{endpoints[0]}\'"
            if (replica_num) > 1:
                distribution += ", ["
            for endpoint in endpoints[1:]:
                distribution += f"\'{endpoint}\',"
            if (replica_num) > 1:
                distribution = distribution[:-1]
            if (replica_num) > 1:
                distribution += "]"
            distribution += ")"
        distribution += "]"
        return Status(), distribution
