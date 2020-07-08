FROM docker.4pd.io/base-image-common:1.0

ENV ROLE ""

USER root
COPY ./bin /app/bin/
COPY ./lib /app/lib/
COPY ./conf /app/conf/
VOLUME ["/app/logs"]
ENTRYPOINT ["/bin/bash", "/app/bin/start.sh"]

