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

if [[ $BASE_DIR == "" ]]; then
    echo "please set 'BASE_DIR' when run script"
    exit 1
fi

if [[ $# -lt 1 ]]; then
    echo "at lease one node is required"
    exit 1
fi
echo "[tablet]"
for host in "$@"; do
    echo "${host}:30221" "${BASE_DIR}/tablet"
done

num=0
echo -e  "\n[nameserver]"
for host in "$@"; do
    echo "${host}:30321" "${BASE_DIR}/nameserver"
    let num=num+1
    if [[ $num -eq 2 ]]; then
        break
    fi
done

echo -e "\n[apiserver]"
for host in "$@"; do
    echo "${host}:39080" "${BASE_DIR}/apiserver"
    break
done

num=0
echo -e "\n[taskmanager]"
for host in "$@"; do
    echo "${host}:39902" "${BASE_DIR}/taskmanager"
    let num=num+1
    if [[ $num -eq 2 ]]; then
        break
    fi
done

num=0
echo -e "\n[zookeeper]"
for host in "$@"; do
    echo "${host}:32181:32888:33888" "${BASE_DIR}/zookeeper"
    let num=num+1
    if [[ $num -eq 3 ]]; then
        break
    fi
done
