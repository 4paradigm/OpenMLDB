#!/bin/bash
ROOT_DIR=$(cd $(dirname $0); pwd)/../..
cd ${ROOT_DIR}/java
mvn package -DskipTests=true -Dscalatest.skip=true -Prun_shade