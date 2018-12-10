FROM centos:7
USER root
RUN yum -y install epel-release
RUN yum -y install python-pip
RUN pip install prometheus_client
COPY ./bin /root/bin/
COPY ./lib /root/lib/
COPY ./tablet.flags /root/conf/
VOLUME ["/root/db", "/root/logs", "/root/recycle"]
WORKDIR /root
ENTRYPOINT ["/bin/bash", "/root/bin/start.sh"]
