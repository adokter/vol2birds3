#!/bin/bash
# set path to scar.py script
export scar=~/git/scar/scar.py
# remove any existing instances of the lambda function
$scar rm -n lambda-docker-vol2bird
# make lambda function named lambda-docker-vol2bird, using 128 Mb memory, from Docker image adokter/lambda_vol2bird
$scar init -s vol2bird_scar.sh -n lambda-docker-vol2bird -m 256 -t 250 --nexrad -es vol2bird adokter/lambda_vol2bird
