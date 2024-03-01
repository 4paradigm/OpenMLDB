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

# Before building an offline image, copy this Dockerfile to the same directory as OpenMLDB firstly

# specify a base version of OpenMLDB docker image
ARG version=0.8.5
FROM ghcr.io/4paradigm/hybridsql:${version}

RUN cd /root && \
    git clone --recurse-submodules https://github.com/4paradigm/OpenMLDB.git