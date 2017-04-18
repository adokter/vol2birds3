#!/usr/bin/python
import boto3
import pytz
from datetime import timedelta
from datetime import datetime

utc=pytz.UTC

client = boto3.client('batch')

mydate='2015/03/01'
myradar='KBRO'
mydays=122

mydatetime = utc.localize(datetime.strptime(mydate, '%Y/%m/%d'))
datetimes = [mydatetime+timedelta(days=x) for x in range(0,mydays)]

for t in datetimes:
   params={'radar':myradar, 'date':t.strftime("%Y/%m/%d")}
   response = client.submit_job(
   jobDefinition='vol2birds3-job:12',
   jobName='bototest',
   jobQueue='spot20-job-queue',
   parameters=params)
   print params

