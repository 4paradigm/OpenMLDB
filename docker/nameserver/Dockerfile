FROM centos:7
COPY ./bin /root/bin/
COPY ./lib /root/lib/
COPY ./nameserver.flags /root/conf/
VOLUME ["/root/db", "/root/logs", "/root/recycle"]
WORKDIR /root
ENTRYPOINT ["/bin/bash", "./bin/start_ns.sh"]
