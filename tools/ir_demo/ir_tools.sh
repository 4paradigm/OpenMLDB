# tools/ir_demo/ir_tools.sh
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

EXPECTED_ARGS=1


if [ $# -ne $EXPECTED_ARGS ]
then
       source_file=$1
else
      source_file="ir_test.cc"
fi;

echo $source_file
ir_file=$source_file".ll"
clang -Os -S -emit-llvm --std=c++11 $source_file -o $ir_file
cat $ir_file
rm $ir_file
