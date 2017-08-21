#!/bin/bash

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

