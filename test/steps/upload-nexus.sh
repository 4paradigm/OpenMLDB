#!/usr/bin/env bash
path=$1
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
