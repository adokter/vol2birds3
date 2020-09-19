from __future__ import print_function

import json
import boto3
from os.path import basename
from datetime import datetime, timedelta

print('Loading function')

batch = boto3.client('batch')

def check_key_existence_in_dictionary(key, dictionary):
    return (key in dictionary) and dictionary[key]

def is_sns_event(event):
    if check_key_existence_in_dictionary('Records', event):
        # Check if the event is an SNS event
        if 'EventSource' in event['Records'][0].keys():
            return event['Records'][0]['EventSource'] == "aws:sns"
        else:
            return False
    else:
        return False

def get_s3_record(event):
    if check_key_existence_in_dictionary('Records', event):
        if len(event['Records']) > 1:
            logger.warning("Multiple records detected. Only processing the first one.")
        record = event['Records'][0]
        if check_key_existence_in_dictionary('s3', record):
            return record['s3']

def get_sns_s3_record(event):
    if check_key_existence_in_dictionary('Records', event):
        if len(event['Records']) > 1:
            print("WARNING: MULTIPLE RECORDS DETECTED. ONLY PROCESSING THE FIRST ONE.")
        record = event['Records'][0]
        if('Sns' in record) and record['Sns']:
            if('Message' in record['Sns']) and record['Sns']['Message']:
                sns_payload=json.loads(record['Sns']['Message'])
                return get_s3_record(sns_payload)

def validate_event(event, context):
    if(is_sns_event(event)):
        s3_record = get_sns_s3_record(event)
        key = basename(s3_record['object']['key'])
        datetime_min = datetime.utcnow() + timedelta(days=-7)
        datetime_key = datetime.strptime(key[4:19], '%Y%m%d_%H%M%S')
        if(key[0] != 'K'):
            print("SCRIPT: NEXRAD file of a radar outside USA, ignoring ...")
            return False
        if(datetime_key < datetime_min):
            print("SCRIPT: NEXRAD file more than a week old, ignoring ...")
            return False
        if(datetime_key.hour < 22 and datetime_key.hour > 16):
            print("SCRIPT: NEXRAD file daytime, ignoring ...")
            return False
        return True
    else:
        return True

def lambda_handler(event, context):
    
    # AWS Batch job and S3 bucket definitions:
    jobQueue="is-birdcast-observed-vol2bird"
    jobDefinition="is-birdcast-observed-vol2bird"
    bucket_out="is-birdcast-observed"
    prefix="output_test"

    # Log the received event (commented out)
    # print("Received event: " + json.dumps(event, indent=2))
    # Get parameters for the SubmitJob call
    # http://docs.aws.amazon.com/batch/latest/APIReference/API_SubmitJob.html

    try:
        if validate_event(event,context):
            s3_record = get_sns_s3_record(event)
            key = basename(s3_record['object']['key'])
            bucket_in = s3_record['bucket']['name']
            
            # set job name equal to the file key
            jobName=key
            
            # construct the AWS Batch parameters to parse
            parameters={'key':key, 'source':bucket_in, 'bucket':bucket_out, 'prefix':prefix, 'clut':"--clut", 'aws':"--aws", 'opts':"NLAYER=50\nHLAYER=100.0"}
            
            # Submit a Batch Job
            response = batch.submit_job(jobQueue=jobQueue, jobName=jobName, jobDefinition=jobDefinition,
                                        parameters=parameters)
            # Log response from AWS Batch (commented out)
            # print("Response: " + json.dumps(response, indent=2))
            
            # Return the jobId
            jobId = response['jobId']
            print("AWS Batch jobId: ", jobId)
            
            return {
                'jobId': jobId
            }
    except Exception as e:
        print(e)
        message = 'Error submitting Batch Job'
        print(message)
        raise Exception(message)

