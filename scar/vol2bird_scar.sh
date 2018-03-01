#!/bin/bash

# 1. A file uploaded to the input folder of the S3 bucket will be made available 
#    for the container in /tmp/$REQUEST_ID/input. The path to the file is in $SCAR_INPUT_FILE
# 2. The file will be processed using vol2bird
# 3. The output bird profile will be stored /tmp/$REQUEST_ID/output, and will be
#    automatically uploaded by the Lambda function to the output folder of the S3 bucket.

OUTPUT_DIR="/tmp/$REQUEST_ID/output"

echo "SCRIPT: Invoked processing script. File available in $SCAR_INPUT_FILE"

FILE_NAME=`basename $SCAR_INPUT_FILE`
OUTPUT_FILE=$OUTPUT_DIR/$FILE_NAME".h5"

# note: date / radar selections occur in the python script that calls this bash script:
# https://github.com/adokter/scar/blob/master/lambda/scarsupervisor.py 

echo "SCRIPT: invoked vol2bird";
vol2bird $SCAR_INPUT_FILE $OUTPUT_FILE > /dev/null
