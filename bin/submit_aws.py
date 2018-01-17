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
   mynight=" "
   mydown='DEALIAS_VRAD=TRUE' #this is the unrelated default option, improve ugly coding here ... trick to bypass that parameters need value
   mystep="10"

   try:
      opts, args = getopt.getopt(argv,"hNDr:d:n:s:q:",["help","night","downsample","radar=","date=","nday=","step=","queue="])
   except getopt.GetoptError:
      print "error: unrecognised arguments"
      print me+' -r <radar> -d <date> -n <nday> -s <step> -q <queue> [--night] [--downsample]'
      print me+' -h | --help'
      sys.exit(2)
   for opt, arg in opts:
      if opt in ('-h', "--help"):
         print 'Usage: '
         print '  '+me+' -r <radar> -d <date> -n <nday> -s <step> -q <queue> [--night] [--downsample]'
         print '  '+me+' -h | --help'
         print '\nOptions:'
         print '  -h --help       Show this screen'
         print '  -r --radar      Specify NEXRAD radar, e.g. KBGM'
         print '  -d --date       Specify the start date in yyyy/mm/dd format'
         print '  -n --nday       Specify the number of days to process, starting from start date'
         print '  -s --step       Specify the time step in minutes'
         print '  -q --queue      Specify the AWS batch job queue to submit to'
         print '  -N --night      Nighttime only'
         print '  -D --downsample Downsample for faster processing'
         sys.exit()
      elif opt in ("-d", "--date"):
         mydate = arg
      elif opt in ("-r", "--radar"):
         myradar = arg
      elif opt in ("-n", "--nday"):
         mydays = int(arg)
      elif opt in ("-s", "--step"):
         mystep = str(arg)
      elif opt in ("-q", "--queue"):
         myqueue = arg
      elif opt in ("-N", "--night"):
         mynight = "--night"
      elif opt in ("-D", "--downsample"):
         mydown = "RESAMPLE=TRUE"
   if not(myradar != '' and mydate != '' and mydays>0):
      print "error: both a radar, date and nday specification required"
      print me+' -r <radar> -d <date> -n <nday> -s <step> -q <queue> [--night] [--downsample]'
      print me+' -h | --help'
      sys.exit()

   utc=pytz.UTC

   client = boto3.client('batch')

   mydatetime = utc.localize(datetime.strptime(mydate, '%Y/%m/%d'))
   datetimes = [mydatetime+timedelta(days=x) for x in range(0,mydays)]

   for t in datetimes:
      #params={'radar':myradar, 'date':t.strftime("%Y/%m/%d"), 'step':mystep, 'opts':mydown, 'night':mynight}
      #note!! removed night parameter
      params={'radar':myradar, 'date':t.strftime("%Y/%m/%d"), 'step':mystep, 'opts':mydown}
      print params
      print "submitting job "+myradar+t.strftime("%Y%m%d")+"..."
      response = client.submit_job(
      jobDefinition='vol2birds3-job:16', #note!! in revision 16 I removed the night parameter
      jobName=myradar+t.strftime("%Y%m%d"),
      jobQueue=myqueue,
      parameters=params)

if __name__ == "__main__":
   main(sys.argv[1:])
