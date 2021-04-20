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
# goto toplevel directory
cd "$(dirname "$0")"
DOC_DIR=$(pwd)

if [[ -d "xml" ]]; then
  rm -rf "xml"
fi

if [[ -d "java" ]]; then
  rm -rf "java"
fi
mkdir "java"

# need to download doxybook2 before run script
if [[ -d "doxybook2_home" ]]; then
  echo "doxybook2 already exist, skip download and unzip"
else
  echo "Start download and unzip doxybook2 tool"
  if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    # download linux doxygen2
    wget https://github.com/matusnovak/doxybook2/releases/download/v1.3.3/doxybook2-linux-amd64-v1.3.3.zip
    unzip doxybook2-linux-amd64-v1.3.3.zip -d doxybook2_home
  else
    wget https://github.com/matusnovak/doxybook2/releases/download/v1.3.3/doxybook2-osx-amd64-v1.3.3.zip
    unzip doxybook2-osx-amd64-v1.3.3.zip -d doxybook2_home
  fi
fi

doxygen
export LC_CTYPE=C &&  export LANG=C && grep -rl "kind=\"enum\"" xml |xargs sed -i "" 's/kind=\"enum\"/kind=\"class\"/g'
export LC_CTYPE=C &&  export LANG=C && grep -rl "::" xml |xargs sed -i "" 's/::/./g'
doxybook2_home/bin/doxybook2 --input xml --output java --config config.json --templates ../template --summary-input SUMMARY.md.tmpl --summary-output SUMMARY.md

