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


FROM centos:7

LABEL org.opencontainers.image.source https://github.com/4paradigm/OpenMLDB

COPY setup_cmake.sh /
# hadolint ignore=DL3031
RUN yum update -y && yum install -y centos-release-scl epel-release && \
    yum install -y devtoolset-8 rh-git227 devtoolset-8-libasan-devel flex doxygen \
    java-1.8.0-openjdk-devel rh-python38-python-devel rh-python38-python-wheel && \
    curl -SLo lcov-1.15-1.noarch.rpm https://github.com/linux-test-project/lcov/releases/download/v1.15/lcov-1.15-1.noarch.rpm && \
    yum localinstall -y lcov-1.15-1.noarch.rpm && \
    yum clean all && rm lcov-1.15-1.noarch.rpm && \
    curl -SLo apache-maven-3.6.3-bin.tar.gz https://mirrors.ocf.berkeley.edu/apache/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.tar.gz && \
    tar xzf apache-maven-3.6.3-bin.tar.gz -C /usr/local --strip-components=1 && \
    curl -SLo thirdsrc.tar.gz https://github.com/jingchen2222/hybridsql-asserts/releases/download/v0.3.1/thirdsrc-2021-05-17.tar.gz && \
    mkdir -p /depends/thirdsrc && \
    tar xzf thirdsrc.tar.gz -C /depends/thirdsrc --strip-components=1 && \
    /setup_cmake.sh "${TARGETARCH}" && \
    rm /setup_cmake.sh && \
    rm ./*.tar.gz

ENV PATH=/opt/rh/rh-git227/root/usr/bin:/opt/rh/rh-python38/root/usr/local/bin:/opt/rh/rh-python38/root/usr/bin:/opt/rh/devtoolset-8/root/usr/bin:/depends/thirdparty/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
ENV LD_LIBRARY_PATH=/depends/thirdparty/lib:/depends/thirdparty/lib64:/opt/rh/httpd24/root/usr/lib64:/opt/rh/rh-python38/root/usr/lib64:/opt/rh/devtoolset-8/root/usr/lib64:/opt/rh/devtoolset-8/root/usr/lib:/opt/rh/devtoolset-8/root/usr/lib64/dyninst
ENV LANG=en_US.UTF-8

WORKDIR /root

CMD [ "/bin/bash" ]

