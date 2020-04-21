#! /bin/sh
#
# style_check.sh

python tools/cpplint.py --output=junit --root=. --recursive --exclude=src/proto/*.pb.* src/* 2> ./style.xml

