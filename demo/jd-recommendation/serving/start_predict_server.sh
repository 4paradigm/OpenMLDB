#! /bin/bash
#
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

# start_predict_server.sh

cd "$(dirname "$0")" || exit 1

echo "start predict server"

cd $(dirname $0)
nohup python3 predict_server.py 127.0.0.1:9080 127.0.0.1:8000 >/tmp/p.log 2>&1 &
sleep 1
