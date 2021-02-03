#! /bin/sh

fesql_branch=$1
if [ -z "${fesql_branch}" ]; then
    fesql_branch="develop"
fi
cd fesql
git checkout ${fesql_branch}
git pull
cd ..