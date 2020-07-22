#!/bin/bash

# build the docker image
docker build -t adokter/vol2birds3 .

# build the docker image including mistnet
docker build -f Dockerfile.vol2bird-mistnet -t adokter/vol2birds3-mistnet .
