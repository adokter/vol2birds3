#! /bin/bash

# 1. A file uploaded to the input folder of the S3 bucket will be made available 
#    for the container in /tmp/$REQUEST_ID/input. The path to the file is in $SCAR_INPUT_FILE
# 2. The file will be processed using vol2bird
# 3. The output bird profile will be stored /tmp/$REQUEST_ID/output, and will be
#    automatically uploaded by the Lambda function to the output folder of the S3 bucket.

OUTPUT_DIR="/tmp/$REQUEST_ID/output"

echo "SCRIPT: Invoked vol2bird. File available in $SCAR_INPUT_FILE"

FILE_NAME=`basename $SCAR_INPUT_FILE`
OUTPUT_FILE=$OUTPUT_DIR/$FILE_NAME".h5"

# do some selection on date
DATE_FILE=${FILE_NAME:4:12}
HOUR_FILE=${FILE_NAME:13:2}
DATE_MIN=`date --date="7 days ago" +"%Y%m%d"`
RADAR=${FILE_NAME:0:4}

if [ ${RADAR:0:1} != "K" ];
then 
    echo "SCRIPT: not a USA radar, ignoring ...";
    exit 1
fi;

if [ $DATE_FILE \< $DATE_MIN ];
then 
    echo "SCRIPT: file over a week old, ignoring...";
    exit 1
fi;

if [ $HOUR_FILE \< 22 ] && [ $HOUR_FILE \> 14 ];
then
    echo "SCRIPT: daytime, ignoring ...";
    exit 1
fi;

vol2bird $SCAR_INPUT_FILE $OUTPUT_FILE > /dev/null
