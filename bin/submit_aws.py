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
   myoptions=''
   mystep="10"
   myjob=''
   mybucket = 'vol2bird'
   myprefix = 'singlepol'
   dryrun = False

   try:
      opts, args = getopt.getopt(argv,"hNDr:d:n:s:o:j:q:b:p:",["help","night","dryrun","radar=","date=","nday=","step=","opts=","job=","queue=","bucket=","prefix="])
   except getopt.GetoptError:
      print "error: unrecognised arguments"
      printSyntax(me)
      sys.exit(2)
   for opt, arg in opts:
      if opt in ('-h', "--help"):
         printSyntax(me)
         print '\nOptions:'
         print '  -h --help       Show this screen'
         print '  -r --radar      Specify NEXRAD radar, e.g. KBGM'
         print '  -d --date       Specify the start date in yyyy/mm/dd format'
         print '  -n --nday       Specify the number of days to process, starting from start date'
         print '  -s --step       Specify the time step in minutes'
         print '  -q --queue      Specify the AWS batch job queue to submit to'
         print '  -j --job        Specify the AWS batch job definition to use'
         print '  -N --night      Nighttime only'
         print '  -o --opts       Options to write to options.conf file read by vol2bird; separate lines by "\\n"'
         print '  -b --bucket     The AWS bucket name where profiles will be stored"'
         print '  -p --prefix     The AWS prefix (i.e. bucket postfix) of the filename for storing profiles"'
         print '  -D --dryrun     Do not execute, dry run only'
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
      elif opt in ("-o", "--opts"):
         myoptions = arg
      elif opt in ("-j", "--job"):
         myjob = arg
      elif opt in ("-b","--bucket"):
         mybucket = arg
      elif opt in ("-p","--prefix"):
         myprefix = arg
      elif opt in ("-D","--dryrun"):
         dryrun = True
   if not(myradar != '' and mydate != '' and mydays>0 and myjob !=''):
      print "error: both a radar, date, job, queue, and nday specification required"
      printSyntax(me)
      sys.exit()

   utc=pytz.UTC

   client = boto3.client('batch')

   mydatetime = utc.localize(datetime.strptime(mydate, '%Y/%m/%d'))
   datetimes = [mydatetime+timedelta(days=x) for x in range(0,mydays)]

   for t in datetimes:
      if myoptions == '' and myprefix == '':
         params={'radar':myradar, 'date':t.strftime("%Y/%m/%d"), 'step':mystep, 'night':mynight, 'bucket':mybucket}
      elif myoptions == '' and myprefix != '':
         params={'radar':myradar, 'date':t.strftime("%Y/%m/%d"), 'step':mystep, 'night':mynight, 'bucket':mybucket, 'prefix':myprefix}
      elif myoptions != '' and myprefix == '':
         params={'radar':myradar, 'date':t.strftime("%Y/%m/%d"), 'step':mystep, 'opts':myoptions, 'night':mynight, 'bucket':mybucket}
      else:
         params={'radar':myradar, 'date':t.strftime("%Y/%m/%d"), 'step':mystep, 'opts':myoptions, 'night':mynight, 'bucket':mybucket, 'prefix':myprefix}
      print params
      print "submitting job "+myradar+t.strftime("%Y%m%d")+" using job definition "+myjob+" and queue "+myqueue+"..."
      if not(dryrun):
         response = client.submit_job(
         jobDefinition=myjob,
         jobName=myradar+t.strftime("%Y%m%d"),
         jobQueue=myqueue,
         parameters=params)

def printSyntax(me):
   print 'Usage: '
   print '  '+me+' -r <radar> -d <date> -n <nday> -j <job> -q <queue> [-s <step>] [-b <bucket>] [-p <prefix>] [--night] [--dryrun]'
   print '  '+me+' -h | --help'

if __name__ == "__main__":
   main(sys.argv[1:])
