#!/bin/bash
cd $(cd $(dirname $0); pwd)/..
find ./src | grep "\.h$\|\.cc$" | xargs -I {} clang-format -i -style=file {}
