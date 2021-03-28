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

set -eE

pushd "$(dirname "$0")"
pushd "$(git rev-parse --show-toplevel)"

if ! command -v shfmt; then
    if [ ! -e bin/shfmt ]; then
        curl --create-dirs -SLo bin/shfmt https://github.com/mvdan/sh/releases/download/v3.2.4/shfmt_v3.2.4_linux_amd64
        chmod +x bin/shfmt
    fi
    shfmt() {
        bin/shfmt "$@"
    }
fi

if ! command -v google-java-format; then
    if [ ! -e bin/google-java-format.jar ]; then
        curl --create-dirs -SLo bin/google-java-format.jar https://github.com/google/google-java-format/releases/download/google-java-format-1.9/google-java-format-1.9-all-deps.jar
    fi
    google-java-format() {
        java -jar bin/google-java-format.jar "$@"
    }
fi

if [ ! -e node_modules/.bin/prettier ]; then
    npm install --save-dev prettier @prettier/plugin-xml
fi
prettier() {
    ./node_modules/.bin/prettier "$@"
}

if ! command -v yapf; then
    python3 -m pip install --user -U yapf
    yapf() {
        ~/.local/bin/yapf "$@"
    }
fi

# download clang-format

for file in $(git ls-files); do
    if [[ $file = *.cc || $file = *.h ]]; then
        clang-format -i -style=file "$file"
    elif [[ $file = *.py ]]; then
        yapf -i --style=google "$file"
    elif [[ $file = *.sh ]]; then
        shfmt -i 4 -w "$file"
    elif [[ $file = *.xml || $file = *.yaml || $file = *.yml || $file = *.json ]]; then
        prettier -w "$file"
    elif [[ $file = *.java ]]; then
        google-java-format -i --aosp "$file"
    fi
done

popd
popd
