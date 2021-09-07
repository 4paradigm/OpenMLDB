#!/usr/bin/env bash

command=$1
if [[ "${command}" == "" ]]; then
    echo "please input command"
    exit 1
fi

echo "command:${command}"

count=0     #记录重试次数
flag=0      # 重试标识，flag=0 表示任务正常，flag 非0 表示需要进行重试
total=10    #总次数
while [ 0 -eq 0 ]
do
    echo ".................. job begin  ..................."
    # ...... 添加要执行的内容，flag 的值在这个逻辑中更改为1，或者不变......
    ${command}
    flag=$?
    echo "flag:${flag}"
    # 检查和重试过程
    if [ ${flag} -eq 0 ]; then     #执行成功，不重试
        echo "--------------- job complete ---------------"
        break;
    else                        #执行失败，重试
        count=$[${count}+1]
        if [ ${count} -eq ${total} ]; then     #指定重试次数，重试超过5次即失败
            echo 'timeout,exit.'
            exit 1
        fi
        echo "...........retry ${count} in 2 seconds .........."
        sleep 2
    fi
done

