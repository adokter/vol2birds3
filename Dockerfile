FROM adokter/vol2bird
RUN \
  apt-get update && apt-get install --no-install-recommends -y \
  python python-pip && \
  rm -rf /var/lib/apt/lists/* && \
  pip install boto pytz astral

ENV INSIDE_DOCKER_CONTAINER Yes

COPY bin/radcp.py /opt/radcp.py
COPY bin/process_day.py /opt/process_day.py
COPY bin/process_file.py /opt/process_file.py
RUN mkdir -p /opt/occult
COPY occult /opt/occult
WORKDIR /opt
