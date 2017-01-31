FROM adokter/vol2bird

RUN \
  apt-get update && apt-get install --no-install-recommends -y \
    ruby && \
  rm -rf /var/lib/apt/lists/* && \
  echo "gem: --no-ri --no-rdoc" > ~/.gemrc && \
  gem install aws-sdk

COPY bin/process_file.rb /opt/process_file.rb

WORKDIR /opt

CMD ["./process_file.rb"]
