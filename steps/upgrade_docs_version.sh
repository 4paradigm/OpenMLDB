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


VERSION=$1
if [[ ! ${VERSION} =~ ^[0-9]\.[0-9]\.[0-9]$ ]]
then
    echo "invalid version ${VERSION}"
    exit 0
fi
echo "new version is ${VERSION}"
MAIN_VERSION=$(echo "${VERSION}" | awk -F '.' '{print $1"."$2}')
echo "main version is ${MAIN_VERSION}"

upgrade_docker() {
    sed -i"" -e "s/4pdosc\/openmldb:[0-9]\.[0-9]\.[0-9]/4pdosc\/openmldb:${VERSION}/g" "$1"
}

upgrade_java_sdk() {
    sed -i"" -e "s/<version>[0-9]\.[0-9]\.[0-9]<\/version>/<version>${VERSION}<\/version>/g" "$1"
    sed -i"" -e "s/<version>[0-9]\.[0-9]\.[0-9]-macos<\/version>/<version>${VERSION}-macos<\/version>/g" "$1"
    sed -i"" -e "s/\`[0-9]\.[0-9]\.[0-9]-macos\`/\`${VERSION}-macos\`/g" "$1"
}

upgrade_install_doc() {
    sed -i"" -e "s/\/v[0-9]\.[0-9]\.[0-9]\//\/v${VERSION}\//g" "$1"
    sed -i"" -e "s/openmldb-[0-9]\.[0-9]\.[0-9]-linux/openmldb-${VERSION}-linux/g" "$1"
    sed -i"" -e "s/openmldb-[0-9]\.[0-9]\.[0-9]-darwin/openmldb-${VERSION}-darwin/g" "$1"
    sed -i"" -e "s/-openmldb[0-9]\.[0-9]\.[0-9]\//-openmldb${VERSION}\//g" "$1"
    components=("ns" "tablet" "apiserver" "taskmanager")
    for component in "${components[@]}"
    do
        sed -i"" -e "s/openmldb-${component}-[0-9]\.[0-9]\.[0-9]/openmldb-${component}-${VERSION}/g" "$1"
    done
}

upgrade_compile_doc() {
    sed -i"" -e "s/OpenMLDB v[0-9]\.[0-9]\.[0-9]/OpenMLDB v${VERSION}/g" "$1"
    sed -i"" -e "s/ v[0-9]\.[0-9]\.[0-9]/ v${VERSION}/g" "$1"
    sed -i"" -e "s/v[0-9]\.[0-9]\.[0-9])/v${VERSION})/g" "$1"
    sed -i"" -e "s/hybridsql:[0-9]\.[0-9]\.[0-9]/hybridsql:${VERSION}/g" "$1"
    sed -i"" -e "s/4pdosc\/hybridsql:[0-9]\.[0-9]/4pdosc\/hybridsql:${MAIN_VERSION}/g" "$1"
    sed -i"" -e "s/-openmldb[0-9]\.[0-9]\.[0-9]\//-openmldb${VERSION}\//g" "$1"
}

find demo/ docs/  -name "*\.md" | while read -r file;
do
    upgrade_docker "$file"
done

upgrade_java_sdk "docs/en/quickstart/java_sdk.md"
upgrade_java_sdk "docs/zh/quickstart/java_sdk.md"

upgrade_install_doc "docs/en/deploy/install_deploy.md"
upgrade_install_doc "docs/zh/deploy/install_deploy.md"

upgrade_compile_doc "docs/en/deploy/compile.md"
upgrade_compile_doc "docs/zh/deploy/compile.md"
