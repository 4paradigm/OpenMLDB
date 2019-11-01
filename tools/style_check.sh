#! /bin/sh
#
# style_check.sh

python tools/cpplint.py --output=junit --root=. --recursive src/* 2> ./style.xml

