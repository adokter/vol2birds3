FROM adokter/vol2bird

RUN \
  apt-get update && apt-get install --no-install-recommends -y \
  python python-pip && \
  rm -rf /var/lib/apt/lists/* && \
  pip install boto pytz astral

COPY bin/radcp.py /opt/radcp.py
COPY bin/process_day.py /opt/process_day.py
COPY occult /opt/occult

WORKDIR /opt
