#! /bin/sh
#
# style_check.sh

python tools/cpplint.py --output=junit --linelength=120000 --root=. --recursive src/*
