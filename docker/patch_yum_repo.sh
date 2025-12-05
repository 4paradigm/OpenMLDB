#!/bin/bash

set -e

sed -i s/mirror.centos.org/vault.centos.org/g /etc/yum.repos.d/*.repo
sed -i s/^#.*baseurl=http/baseurl=http/g /etc/yum.repos.d/*.repo
sed -i s/^mirrorlist=http/#mirrorlist=http/g /etc/yum.repos.d/*.repo

if [[ "$(arch)" = "aarch64" ]]; then
    sed -i s/vault.centos.org\\/centos/vault.centos.org\\/altarch/g /etc/yum.repos.d/*.repo
fi
