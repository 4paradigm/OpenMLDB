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

# $1 should be 1(multi dimension) or 0(one dimension)
# $2 should be regex for filter testcases
ulimit -c unlimited
if [ -f "test-common/integrationtest/setup.sh" ]
then
    export runlist=$2
    export norunlist=$3
    sh test-common/integrationtest/runall.sh $1 $2 $3
fi
