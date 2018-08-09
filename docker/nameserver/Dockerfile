FROM centos:7
COPY ./bin /root/bin/
COPY ./nameserver.flags /root/conf/
VOLUME ["/root/db", "/root/logs", "/root/recycle"]
WORKDIR /root
ENTRYPOINT ["/bin/bash", "./bin/start_ns.sh"]
