#! /bin/sh
#
# style_check.sh

mkdir -p ./reports
python3 tools/cpplint.py --linelength=100 --output=junit --root=. --exclude='src/proto/*' --exclude='src/storage/beans/*' --exclude='src/version.h' --exclude='src/config.h' --recursive src/* 2> ./reports/style.xml

