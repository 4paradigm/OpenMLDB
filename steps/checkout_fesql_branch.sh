#! /bin/sh

fesql_branch=$1
if [ -z "${fesql_branch}" ]; then
    git submodule update
else
    cd fesql
    git checkout ${fesql_branch}
    git pull
    cd ..
fi