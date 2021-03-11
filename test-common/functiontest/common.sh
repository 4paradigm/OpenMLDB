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

function decompress() {
    tarfile=$1
    dstdir=$2
    dstname=$3
    dstroot=`dirname  ${dstdir}`
    if [ ! -d ${dstroot} ]; then
        execshell "mkdir -p ${dstroot}"
    fi
    execshell "tar zxvf ${tarfile} -C ${dstroot}"
    if [ ${dstroot}/${dstname} -ne ${dstdir} ]; then
        execshell "mv ${dstroot}/${dstname} ${dstdir}"
    fi
}

function color() {
    local var=$1
    shift
    echo "\e[$var;1m$*\e[m"
}
function log_notice() {
    echo -e "[`color 32 NOTICE`]: `date +"%Y-%m-%d %H:%M:%S"`: $*" 1>&2
}

function log_warn() {
    echo -e "[`color 35 WARNING`]: `date +"%Y-%m-%d %H:%M:%S"`: $*" 1>&2
}

function log_fatal() {
    echo -e "[`color 31 FATAL`]: `date +"%Y-%m-%d %H:%M:%S"`: $*" 1>&2
}

function execshell() {
    local be_color=48
    log_notice "`color $be_color BEGIN` [`color 39 $@`]"
    eval $@
    [[ $? != 0 ]] && {
        log_fatal "`color $be_color END` [`color 39 $@`] [`color 31 FAIL`]"
        exit 1
    }
    log_notice "`color $be_color END` [`color 39 $@`] [`color 32 SUCCESS`]"
    return 0
}

