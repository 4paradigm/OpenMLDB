#!/bin/bash

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

set -x -e

if [[ "$OSTYPE" = "darwin"* ]]; then
    pkill -9 -x -l openmldb || exit 0
else
    pgrep -a -f "openmldb.*onebox.*" | awk '{print $1}' | xargs -I {} kill -9 {}
fi

