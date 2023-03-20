#! /usr/bin/env bash
# shellcheck disable=SC1091

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

if [[ $# -lt 2 ]]; then
    echo "invalid args"
    exit 1
fi
BASE_DIR=$1
OLD_IFS="$IFS"
IFS=","
hosts=($2)
IFS="$OLD_IFS"

echo "[tablet]"
for host in "${hosts[@]}"; do
    echo "${host}:30221" "${BASE_DIR}/tablet"
done

num=0
echo -e  "\n[nameserver]"
for host in "${hosts[@]}"; do
    echo "${host}:30321" "${BASE_DIR}/nameserver"
    (( num=num+1 ))
    if [[ $num -eq 2 ]]; then
        break
    fi
done

echo -e "\n[apiserver]"
for host in "${hosts[@]}"; do
    echo "${host}:39080" "${BASE_DIR}/apiserver"
    break
done

num=0
echo -e "\n[taskmanager]"
for host in "${hosts[@]}"; do
    echo "${host}:39902" "${BASE_DIR}/taskmanager"
    (( num=num+1 ))
    if [[ $num -eq 2 ]]; then
        break
    fi
done

num=0
echo -e "\n[zookeeper]"
for host in "${hosts[@]}"; do
    echo "${host}:32181:32888:33888" "${BASE_DIR}/zookeeper"
    (( num=num+1 ))
    if [[ $num -eq 3 ]]; then
        break
    fi
done
