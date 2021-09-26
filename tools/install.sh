#!/bin/bash
if ! [ -x "$(command -v java)" ]; then
  echo 'Error: java is not installed.' >&2
  exit 1
fi

if ! [ -x "$(command -v wget)" ]; then
  echo 'Error: wget is not installed.' >&2
  exit 1
fi

echo 'install zookeeper'
FILE=zookeeper-3.4.14.tar.gz
if [ -e "$FILE" ]; then
    echo "$FILE exist"
else 
  wget https://archive.apache.org/dist/zookeeper/zookeeper-3.4.14/zookeeper-3.4.14.tar.gz
  tar -zxvf zookeeper-3.4.14.tar.gz
  cd zookeeper-3.4.14
  cp conf/zoo_sample.cfg conf/zoo.cfg
  cd ..
fi

echo 'install nameserver'
FILE=openmldb-0.2.2-linux.tar.gz
if [ -e "$FILE" ]; then
  echo "$FILE exist"
else
  wget https://github.com/4paradigm/OpenMLDB/releases/download/0.2.2/openmldb-0.2.2-linux.tar.gz
  if [ -e "$FILE" ]; then
    tar -zxvf openmldb-0.2.2-linux.tar.gz
    mv openmldb-0.2.2-linux openmldb-ns-0.2.2
    echo 'install tablet'
    tar -zxvf openmldb-0.2.2-linux.tar.gz
    mv openmldb-0.2.2-linux openmldb-tablet-2.2.0
  else
    echo 'download fail ; try script again'
    exit 1
  fi
fi

echo 'install complete'
 

