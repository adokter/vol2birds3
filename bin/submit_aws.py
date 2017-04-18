#!/usr/bin/python
import boto3

client = boto3.client('batch')

params={'radar':'KBRO', 'date':'2015/05/10'}

response = client.submit_job(
    jobDefinition='vol2birds3-job:12',
    jobName='bototest',
    jobQueue='spot20-job-queue',
    parameters=params
)

print(response)
