# docker/install_ssh.sh
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

#! /bin/sh
#
# install_ssh.sh
## Install the openssh-server and epel-release
##
yum -y install openssh-server epel-release
yum -y install pwgen
rm -f /etc/ssh/ssh_host_ecdsa_key /etc/ssh/ssh_host_rsa_key
ssh-keygen -q -N "" -t dsa -f /etc/ssh/ssh_host_ecdsa_key
ssh-keygen -q -N "" -t rsa -f /etc/ssh/ssh_host_rsa_key
sed -i "s/#UsePrivilegeSeparation.*/UsePrivilegeSeparation no/g"
/etc/ssh/sshd_config
sed -i "s/UsePAM.*/UsePAM yes/g" /etc/ssh/sshd_config
ssh-keygen -A
echo "root:root" | chpasswd
