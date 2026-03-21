FROM rclone/rclone:1.73.2 AS rclone-stage

FROM python:3.11.12-alpine3.21
COPY --from=rclone-stage /usr/local/bin/rclone /usr/local/bin/rclone

WORKDIR /

ADD . / ./
ADD https://raw.githubusercontent.com/debridmediamanager/zurg-testing/f41746b6f6142c773ae86c6059e6e6338d864ba0/config.yml /zurg/
ADD https://raw.githubusercontent.com/debridmediamanager/zurg-testing/f41746b6f6142c773ae86c6059e6e6338d864ba0/scripts/plex_update.sh /zurg/

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
