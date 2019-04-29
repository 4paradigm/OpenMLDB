FROM docker.4pd.io/base-image-common:1.0

USER root
COPY ./bin /app/bin/
COPY ./lib /app/lib/
COPY ./tablet.flags /app/conf/
VOLUME ["/app/db", "/app/logs", "/app/recycle"]
ENTRYPOINT ["/bin/bash", "/app/bin/start.sh"]
