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

FROM ubuntu:22.04

ARG ZETASQL_VERSION=0.3.4
ARG THIRDPARTY_VERSION=0.7.1
ARG TARGETARCH


LABEL org.opencontainers.image.source https://github.com/4paradigm/OpenMLDB

COPY ./*.sh /

RUN apt update -y && \
    apt install -y build-essential python3-dev openjdk-11-jdk flex doxygen lcov git && \
    apt clean cache && \
    curl -Lo zookeeper.tar.gz https://archive.apache.org/dist/zookeeper/zookeeper-3.4.14/zookeeper-3.4.14.tar.gz && \
    mkdir -p /deps/src && \
    tar xzf zookeeper.tar.gz -C /deps/src && \
    /setup_deps.sh -z "$ZETASQL_VERSION" -t "$THIRDPARTY_VERSION" && \
    rm -v /*.sh

ENV THIRD_PARTY_DIR=/deps/usr
ENV THIRD_PARTY_SRC_DIR=/deps/src
ENV LD_LIBRARY_PATH=/deps/usr/lib:/deps/usr/lib64:$LD_LIBRARY_PATH
ENV LANG=en_US.UTF-8
ENV ZETASQL_VERSION=$ZETASQL_VERSION
ENV THIRDPARTY_VERSION=$THIRDPARTY_VERSION
