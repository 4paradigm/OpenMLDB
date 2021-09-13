#!/usr/bin/env bash

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

count=0     #记录重试次数
flag=0      # 重试标识，flag=0 表示任务正常，flag 非0 表示需要进行重试
total=10    #总次数
while [ 0 -eq 0 ]
do
    echo ".................. job begin  ..................."
    # ...... 添加要执行的内容，flag 的值在这个逻辑中更改为1，或者不变......
    git clone -b feat/modify_sdk_pom https://github.com/4paradigm/OpenMLDB.git
    flag=$?
    echo "flag:${flag}"
    # 检查和重试过程
    if [ ${flag} -eq 0 ]; then     #执行成功，不重试
        echo "--------------- job complete ---------------"
        break;
    else                        #执行失败，重试
        count=$((count+1))
        if [ ${count} -eq ${total} ]; then     #指定重试次数，重试超过5次即失败
            echo 'timeout,exit.'
            exit 1
        fi
        echo "...........retry ${count} in 2 seconds .........."
        sleep 2
    fi
done

