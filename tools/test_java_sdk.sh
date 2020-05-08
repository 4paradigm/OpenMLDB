#!/bin/bash
mkdir -p build && cd build 
cmake .. && make fesql_proto && make fesql_parser && make java_package

cd ../java
mvn test

