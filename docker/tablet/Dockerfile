FROM centos:7
COPY ./bin /root/bin/
COPY ./lib /root/lib/
COPY ./tablet.flags /root/conf/
COPY ./py_env /root/py_env/
VOLUME ["/root/db", "/root/logs", "/root/recycle"]
WORKDIR /root
ENTRYPOINT ["/bin/bash", "/root/bin/start.sh"]
