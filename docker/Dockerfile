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


FROM centos:7 AS base

LABEL org.opencontainers.image.source https://github.com/4paradigm/HybridSE

# hadolint ignore=DL3031
RUN yum update -y && yum install -y centos-release-scl epel-release && yum clean all

RUN yum install -y devtoolset-7 rh-git218 devtoolset-7-libasan-devel flex \
    autoconf automake unzip bc expect libtool \
    java-1.8.0-openjdk-devel lcov rh-python38-python-devel && \
    yum clean all

COPY --chown=root:root etc/profile.d/enable-rh.sh /etc/profile.d/

FROM base AS builder

RUN yum install -y gettext byacc xz tcl cppunit-devel && \
    yum clean all

WORKDIR /depends

COPY --chown=root:root *.sh ./

RUN bash fetch_resource.sh

RUN bash install_deps.sh

RUN tar cvzf thirdparty.tar.gz thirdparty/

FROM base

COPY etc/profile.d/enable-thirdparty.sh /etc/profile.d/
COPY --from=builder /usr/local/ /usr/local/

WORKDIR /depends/thirdsrc
COPY --from=builder /depends/thirdsrc/scala-2.12.8.rpm ./
RUN rpm -i scala-2.12.8.rpm && rm ./*.rpm

# use compressed in order to reduce image size
# hadolint ignore=DL3010
COPY --from=builder /depends/thirdparty.tar.gz /depends/
COPY --from=builder /depends/thirdsrc/zookeeper-3.4.14/ /depends/thirdsrc/zookeeper-3.4.14/
COPY --from=builder /opt/maven/ /opt/maven/

RUN yum install -y rh-python38-python-wheel && yum clean all

ENV PATH=/opt/rh/rh-git218/root/usr/bin:/opt/rh/rh-python38/root/usr/local/bin:/opt/rh/rh-python38/root/usr/bin:/opt/rh/devtoolset-7/root/usr/bin:/opt/maven/bin:/depends/thirdparty/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
ENV LD_LIBRARY_PATH=/opt/rh/httpd24/root/usr/lib64:/opt/rh/rh-python38/root/usr/lib64:/opt/rh/devtoolset-7/root/usr/lib64:/opt/rh/devtoolset-7/root/usr/lib:/opt/rh/devtoolset-7/root/usr/lib64/dyninst
ENV LANG=en_US.UTF-8

WORKDIR /root

CMD [ "/bin/bash" ]

