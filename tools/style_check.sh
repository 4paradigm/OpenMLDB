#! /bin/sh
#
# style_check.sh

python tools/cpplint.py --output=junit --root=. --exclude='src/proto/*' --exclude='src/storage/beans/*' --exclude='src/version.h' --exclude='src/config.h' --recursive src/* 2> ./style.xml

