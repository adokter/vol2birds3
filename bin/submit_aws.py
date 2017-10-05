#!/usr/bin/python
import sys
import getopt
import boto3
import pytz
from datetime import timedelta
from datetime import datetime

def main(argv):
   me = sys.argv[0] 
   myradar=''
   mydate=''
   mydays=0
   myqueue=''

   try:
      opts, args = getopt.getopt(argv,"hr:d:n:q:",["help","radar=","date=","nday=","queue="])
   except getopt.GetoptError:
      print "error: unrecognised arguments"
      print me+' -r <radar> -d <date> -n <nday> -q <queue>'
      print me+' -h | --help'
      sys.exit(2)
   for opt, arg in opts:
      if opt in ('-h', "--help"):
         print 'Usage: '
         print '  '+me+' -r <radar> -d <date> -n <nday> -q <queue>'
         print '  '+me+' -h | --help'
         print '\nOptions:'
         print '  -h --help     Show this screen'
         print '  -r --radar    Specify NEXRAD radar, e.g. KBGM'
         print '  -d --date     Specify the start date in yyyy/mm/dd format'
         print '  -n --nday     Specify the number of days to process, starting from start date'
         print '  -q --queue    Specify the AWS batch job queue to submit to'
         sys.exit()
      elif opt in ("-d", "--date"):
         mydate = arg
      elif opt in ("-r", "--radar"):
         myradar = arg
      elif opt in ("-n", "--nday"):
         mydays = int(arg)
      elif opt in ("-q", "--queue"):
         myqueue = arg
   if not(myradar != '' and mydate != '' and mydays>0):
      print "error: both a radar, date and nday specification required"
      print me+' -r <radar> -d <date> -n <nday> -q <queue>'
      print me+' -h | --help'
      sys.exit()

   utc=pytz.UTC

   client = boto3.client('batch')

   mydatetime = utc.localize(datetime.strptime(mydate, '%Y/%m/%d'))
   datetimes = [mydatetime+timedelta(days=x) for x in range(0,mydays)]

   for t in datetimes:
      params={'radar':myradar, 'date':t.strftime("%Y/%m/%d"), 'opts':'RESAMPLE=TRUE'}
      print "submitting job "+myradar+t.strftime("%Y%m%d")+"..."
      response = client.submit_job(
      jobDefinition='vol2birds3-job:13',
      jobName=myradar+t.strftime("%Y%m%d"),
      jobQueue=myqueue,
      parameters=params)

if __name__ == "__main__":
   main(sys.argv[1:])
