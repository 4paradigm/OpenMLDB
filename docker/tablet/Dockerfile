FROM centos:7
USER root
COPY ./bin /root/bin/
COPY ./lib /root/lib/
COPY ./tablet.flags /root/conf/
VOLUME ["/root/db", "/root/logs", "/root/recycle"]
WORKDIR /root
ENTRYPOINT ["/bin/bash", "/root/bin/start.sh"]
