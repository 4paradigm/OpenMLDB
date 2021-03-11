#!/usr/bin/env bash
path=$1
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

if [[ 'x'$path == 'x' ]]; then
    echo 'no paths given, exited'
    exit -1
fi

if [[ ! -e $path ]]; then
    echo "path $path not exist, exited"
    exit -1
fi

if [[ ! -r $path ]]; then
    echo "path $path not readable, exited"
    exit -1
fi

if [[ -L $path ]]; then
    echo "file $path is a link, will upload the real file"
fi

if [[ -f $path ]]; then
    curl  --user 'admin:3mJArND3LdOz' --upload-file "$path" https://nexus.4pd.io/repository/raw-hosted/
    echo "file $path uploaded to https://nexus.4pd.io/repository/raw-hosted/$path"
    exit 0
fi

if [[ -d $path ]]; then
    IFS=$'\n'
    files=`find $path`
    for f in $files
    do
        curl  --user 'deploy:GlW5SRo1TC3q' --upload-file "$f" https://nexus.4pd.io/repository/raw-hosted/"$f"
    done
    echo "files uploaded to: https://nexus.4pd.io/repository/raw-hosted/$path"
fi
