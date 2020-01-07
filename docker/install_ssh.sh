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

