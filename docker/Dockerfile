FROM centos:7
RUN yum group install -y "Development Tools"
RUN yum install -y centos-release-scl
RUN yum install -y devtoolset-8-gcc devtoolset-8-gcc-c++
RUN yum install -y wget texinfo which
RUN yum install -y python-devel.x86_64
RUN wget http://pkg.4paradigm.com/fesql/cmake-3.15.4.tar.gz  && tar -zxvf cmake-3.15.4.tar.gz && cd cmake-3.15.4 && ./bootstrap && gmake -j4 && gmake install
RUN yum install perl-Digest-MD5 -y
RUN wget http://pkg.4paradigm.com/fesql/lcov-1.14.tar.gz  && tar -zxvf lcov-1.14.tar.gz && cd lcov-1.14 && make install
ADD . /depends/
RUN cd /depends && source /opt/rh/devtoolset-8/enable && bash install_deps.sh
RUN cd /depends && source /opt/rh/devtoolset-8/enable && bash install_llvm.sh
RUN cd /depends && source /opt/rh/devtoolset-8/enable && bash install_arrow.sh
RUN cd /depends && source /opt/rh/devtoolset-8/enable && bash install_gdb.sh
RUN cd /depends && source /opt/rh/devtoolset-8/enable && bash install_ssh.sh

