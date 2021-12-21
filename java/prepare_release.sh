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
#  use this script to:
#  1. tweak version number before publish to maven central
#  2. write a bump version number commit quickly
#
# Usage:
#  ./prepare_release.sh $VERSION_NUMBER
#
#  acceptable version style
#  - release version: '0.1.4'
#  - snapshot version: '0.1.4-SNAPSHOT'
#  - beta/alpha: '0.12.2.beta1', '0.12.2.alpha2', will convert released as '0.12.2-SNAPSHOT'
#  - a optional debug number come after base version, 0.12.2.33.alpha -> 0.12.2.33-SNAPSHOT
#  - string literal 'main': it is treated as a push to main, take version number from openmldb-parent
#
# Environment variables:
# - IS_MAC_VARIANT: if setted, hybridse-native & openmldb-native will set to macos variant
#
# in case the version number passed in (extracted from tag) is in wrong style, the script will
# replace any string after 'x.x.x' with '-SNAPSHOT', to avoid publish directly into maven central

set -eE

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

if [ -z "$1" ]; then
    echo -e "Usage: $0 \$VERSION.\n\terror: version number required"
    exit 1
fi

cd "$(dirname "$0")"

if [[ -n $CI ]]; then
  MAVEN_FLAGS='--batch-mode'
fi

IS_MAC_VARIANT=${IS_MAC_VARIANT:-}

VERSION=$1

if [[ $VERSION = 'main' ]]; then
  echo -e "${GREEN}not release from a tag push, $0 will get versions from code${NC}"
  # get version number from code
  VERSION=$(mvn $MAVEN_FLAGS help:evaluate -Dexpression=project.version -q -DforceStdout)
fi
# rm semVer number from VERSION of 3 numbers MAJOR.MINOR.PATCH
#  0.1.2 -> ''
#  0.1.2-SNAPSHOT -> '-SNAPSHOT'
#  0.1.2.beta1    -> '.beta1'
#  0.2.1.0928     -> '.0928'
# shellcheck disable=SC2001
SUFFIX_VERSION=$(echo "$VERSION" | sed -e 's/^[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*//')
# shellcheck disable=SC2001
DEBUG_SUFFIX_VERSION=$(echo "$SUFFIX_VERSION" | sed -e 's/\.[0-9][0-9]*//')
# get BASE VERSION by rm suffix version
if [[ "$DEBUG_SUFFIX_VERSION" != "$SUFFIX_VERSION" ]] ; then
  DEBUG_NO=${SUFFIX_VERSION%"$DEBUG_SUFFIX_VERSION"}
fi

BASE_VERSION=${VERSION%"$SUFFIX_VERSION"}
if [[ -n $SUFFIX_VERSION ]] ; then
    SUFFIX_VERSION=-SNAPSHOT
fi

if [[ -z $BASE_VERSION ]]; then
  echo -e "${RED}invalid version number inputed: $VERSION${NC}"
  exit 1
fi

# VERSION             -> BASE_VERSION    DEBUG_NO    SUFFIX_VERSION
# 0.1.2               -> 0.1.2
# 0.1.2-SNAPSHOT      -> 0.1.2                       SNAPSHOT
# 0.1.2.0928          -> 0.2.1           .0928       SNAPSHOT
# 0.1.2.0928ABC       -> 0.2.1           .0928       SNAPSHOT
# 0.1.2.0928-XXXXXXXX -> 0.2.1           .0928       SNAPSHOT
# 0.1.2.ABC           -> 0.2.1                       SNAPSHOT
# 0.1.2-0928          -> 0.2.1                       SNAPSHOT
echo "BASE_VERSION: ${BASE_VERSION}, DEBUG_NO: ${DEBUG_NO}, SUFFIX_VERSION: ${SUFFIX_VERSION}"

JAVA_VERSION="$BASE_VERSION${DEBUG_NO}$SUFFIX_VERSION"

if [[ -z $IS_MAC_VARIANT ]]; then
  VARIANT_VERSION="$BASE_VERSION${DEBUG_NO}$SUFFIX_VERSION"
else
  VARIANT_VERSION="$BASE_VERSION${DEBUG_NO}-macos$SUFFIX_VERSION"
fi

echo -e "${GREEN}setting project version to: $JAVA_VERSION, setting hybridse-native & openmldb-native to $VARIANT_VERSION${NC}"

mvn $MAVEN_FLAGS versions:set -DnewVersion="$JAVA_VERSION"

# those module has macOS variant so do not inherit number from openmldb-parent
mvn $MAVEN_FLAGS versions:set-property -Dproperty="variant.native.version" -DnewVersion="$VARIANT_VERSION"
