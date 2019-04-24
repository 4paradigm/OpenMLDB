FROM docker.4pd.io/base-image-common:1.0

COPY ./bin /app/bin/
COPY ./lib /app/lib/
COPY ./nameserver.flags /app/conf/
VOLUME ["/app/db", "/app/logs", "/app/recycle"]
ENTRYPOINT ["/bin/bash", "./bin/start_ns.sh"]
