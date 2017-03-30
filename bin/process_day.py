#!/usr/bin/python
import sys
import boto
import os
import shutil
import gzip
import tempfile
import radcp
import getopt
from subprocess import call
from shutil import copyfile

def main(argv):
   if len(argv) == 0:
      # no input arguments, check whether an ARGS environment variable is set
      if not "ARGS" in os.environ:
         print >> sys.stderr, "environment variable 'ARGS' not found, aborting"
         sys.exit()

      # get the contents of the ARGS environment variable
      args = os.environ["ARGS"]
      argslist=args.split()
   else:
      argslist = argv

   me = sys.argv[0] 

   radar=''
   date = ''
   night = False
   step = 0
   zipQ = False

   try:
      opts, args = getopt.getopt(argslist,"hngr:d:s:",["night","gzip","radar=","date=","step="])
   except getopt.GetoptError:
      print "error: unrecognised arguments"
      print me+' -r <radar> -d <date> [--night] [--gzip] [--step <mins>]'
      print me+' -h | --help'
      sys.exit(2)
   for opt, arg in opts:
      if opt == '-h':
         print 'Usage: '
         print '  '+me+' -r <radar> -d <date> [--night] [--gzip] [--step <mins>]'
         print '  '+me+' -h | --help'
         print '\nOptions:'
         print '  -h --help     Show this screen'
         print '  -r --radar    Specify NEXRAD radar, e.g. KBGM'
         print '  -g --gzip     Compress output'
         print '  -d --date     Specify date in yyyy/mm/dd format'
         print '  -n --night    If set, only download nighttime data'
         print '  -s --step     Minimum timestep in minutes between consecutive polar volumes [default: 0]'
         sys.exit()
      elif opt in ("-n", "--night"):
         night = True
      elif opt in ("-g", "--gzip"):
         zipQ = True
      elif opt in ("-d", "--date"):
         date = arg
      elif opt in ("-r", "--radar"):
         radar = arg
      elif opt in ("-s", "--step"):
         step = float(arg)
   if not(radar != '' and date != ''):
      print "error: both a radar and date specification required"
      print me+' -r <radar> -d <date> [--night] [--gzip] [--step <mins>]'
      print me+' -h | --help'
      sys.exit()

   # store the current working directory
   cwd = os.getcwd()

   # make a temporary directory to store the radar data to be downloaded
   tmppath=tempfile.mkdtemp()
   # change the working directory to the new temporary directory
   os.chdir(tmppath)

   # copy the radar files, using radcp with arguments split as a list
   # keep only the radcp arguements
   argscp=argslist
   if '--gzip' in argscp:
      argscp.remove('--gzip')
   elif '--g' in argscp:
      argscp.remove('--g')
   radcp.main(argscp)

   # count the number of files
   NFile = len(os.listdir(tmppath))
   if NFile == 0:
      print >> sys.stderr, 'no files found'
      shutil.rmtree(tmppath)
      sys.exit()

   # get the file list of polar volumes to be processed
   pvols = sorted(os.listdir(tmppath))

   # construct output filename from input argument string
   fout=radar+date+".txt"
   fout=fout.replace('/','')

   with open(fout, "a") as myfile:
      for pvol in pvols:
         localfile = os.path.basename(pvol)
         # process the volume file with vol2bird, write to myfile
         call(["vol2bird",localfile],stdout=myfile)
      # compress myfile and copy it to original working directory
      myfile.close()
      with open(fout, "r") as myfile:
         if zipQ:
            with gzip.open(cwd + "/" + fout + ".gz", 'wb') as zipfile:
               shutil.copyfileobj(myfile, zipfile)
               zipfile.close()
         else:
            with open(cwd + "/" + fout, 'wb') as nozipfile:
               shutil.copyfileobj(myfile, nozipfile)
               nozipfile.close()
         myfile.close()

   # clean up 
   shutil.rmtree(tmppath)

#data_file = os.environ["DATAFILE"]
#output_bucket = os.environ["DEST_BUCKET"]
#localfile = os.path.basename(data_file)
#output_file=localfile+'.h5'

#s3 = boto.connect_s3()

#bucket = s3.get_bucket('noaa-nexrad-level2')
#s3key = bucket.get_key(data_file)
#s3key.get_contents_to_filename(localfile)

#call(["vol2bird",localfile,output_file])

if __name__ == "__main__":
   main(sys.argv[1:])
