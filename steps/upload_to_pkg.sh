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

#! /bin/sh
#!/usr/bin/env bash
 
SCRIPT_NAME=$(basename "$0")
 
function usage() {
    echo "./$SCRIPT_NAME <URL> <file_path>"
}
 
if [[ $# < 2 ]]; then
    usage
    exit 1
fi
 
URL=$1
FILE=$2
 
echo "uploading $FILE to $URL"
 
CSRF=$(curl $URL 2>/dev/null| grep '"token"' | sed 's:.*"token">\(.*\)</span>:\1:')
FILENAME=$(basename $FILE)
curl -XPOST $URL -H "Token: $CSRF" -H 'Upload: true'  -F "$FILENAME=@$FILE"
