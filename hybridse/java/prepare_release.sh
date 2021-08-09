#!/bin/bash

# prepare for maven release, it tweak pom.xml based on a given version number
#  Note: version number should be SemVer (no suffix), e.g. 0.1.4
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

set -eE

if [ -z "$1" ]; then
    echo -e "Usage: $0 \$VERSION.\n\terror: version number required"
    exit 1
fi

cd "$(dirname "$0")"

VERSION=$1
mvn versions:set -DnewVersion="$VERSION"

# rm '-SNAPSHOT' suffix in version.base
BASE_VERSION=${VERSION%-SNAPSHOT}
SUFFIX_VERSION=${VERSION#$BASE_VERSION}
mvn versions:set-property -Dproperty="project.version.base" -DnewVersion="$BASE_VERSION"
mvn versions:set-property -Dproperty="project.version.suffix" -DnewVersion="$SUFFIX_VERSION"
