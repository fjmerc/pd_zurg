FROM rclone/rclone:latest AS rclone-stage

FROM python:3.11-alpine
COPY --from=rclone-stage /usr/local/bin/rclone /usr/local/bin/rclone

WORKDIR /

ADD . / ./
ADD https://raw.githubusercontent.com/debridmediamanager/zurg-testing/main/config.yml /zurg/
ADD https://raw.githubusercontent.com/debridmediamanager/zurg-testing/main/plex_update.sh /zurg/

ENV \
  XDG_CONFIG_HOME=/config \
  TERM=xterm

RUN \
  apk add --update --no-cache gcompat libstdc++ libxml2-utils curl tzdata nano ca-certificates wget fuse3 python3 build-base py3-pip python3-dev linux-headers ffmpeg rust cargo && \
  ln -sf python3 /usr/bin/python && \
  mkdir /log && \
  python3 -m venv /venv && \
  source /venv/bin/activate && \
  pip3 install --upgrade pip && \
  pip3 install -r /plex_debrid/requirements.txt && \
  pip3 install -r /requirements.txt

HEALTHCHECK --interval=60s --timeout=10s \
  CMD ["/bin/sh", "-c", "source /venv/bin/activate && python /healthcheck.py"]
ENTRYPOINT ["/bin/sh", "-c", "source /venv/bin/activate && python /main.py"]