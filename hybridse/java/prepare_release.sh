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

# prepare for maven release, it tweak pom.xml based on a given version number
#  acceptable version style
#  - release version: '0.1.4'
#  - snapshot version: '0.1.4-SNAPSHOT'
#  not acceptable version style
#  - beta/alpha: '0.12.2.beta1', '0.12.2.alpha2', currently not defined for those version number
#
# in case the version number passed in (extracted from tag) is in wrong style, the script will
# replace any string after 'x.x.x' with '-SNAPSHOT', to avoid publish directly into maven central

set -eE

shopt -s extglob

if [ -z "$1" ]; then
    echo -e "Usage: $0 \$VERSION.\n\terror: version number required"
    exit 1
fi

cd "$(dirname "$0")"

VERSION=$1
# rm semVer number from VERSION
SUFFIX_VERSION=${VERSION#+([0-9]).+([0-9]).+([0-9])}
# get BASE VERSION by rm suffix version
BASE_VERSION=${VERSION%$SUFFIX_VERSION}
if [[ -n ${VERSION#$BASE_VERSION} ]] ; then
    SUFFIX_VERSION=-SNAPSHOT
fi

JAVA_VERSION="$BASE_VERSION$SUFFIX_VERSION"

mvn versions:set -DnewVersion="$JAVA_VERSION"

mvn versions:set-property -Dproperty="project.version.base" -DnewVersion="$BASE_VERSION"
mvn versions:set-property -Dproperty="project.version.suffix" -DnewVersion="$SUFFIX_VERSION"
