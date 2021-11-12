#! /bin/sh
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

#
# package.sh
#

set -e
VERSION=${1:-snapshot}
OS=${2:-linux}
package=openmldb-${VERSION}-${OS}
echo "package name: ${package}"

rm -rf "${package}" || :
mkdir "${package}" || :
cp -r release/conf "${package}"/conf
cp -r release/bin "${package}"/bin

cp -r tools "${package}"/tools
ls -l build/bin/
cp -r build/bin/openmldb "${package}"/bin/openmldb

cd "${package}"/bin
cd ../..
tar -cvzf "${package}".tar.gz "${package}"
echo "package at ./${package}"
