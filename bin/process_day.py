#!/usr/bin/python
import sys
import boto
import os
import shutil
import tempfile
import radcp
from subprocess import call
from shutil import copyfile

sys.path.append("/usr/local/vol2bird/bin/")

# check that the ARGS environment variable is set
if not "ARGS" in os.environ:
   print "environment variable 'ARGS' not found, aborting"
   sys.exit()

# get the contents of the ARGS environment variable
args = os.environ["ARGS"]

# store the current working directory
cwd = os.getcwd()

# make a temporary directory to store the radar data to be downloaded
tmppath=tempfile.mkdtemp()
# change the working directory to the new temporary directory
os.chdir(tmppath)
print tmppath

# copy the radar files
radcp.main(args.split())
# count then number of files
NFile = len(os.listdir(tmppath))

f = open("blah.txt", "w")

pvols = os.listdir(tmppath)
with open("test.txt", "a") as myfile:
   for pvol in pvols:
      localfile = os.path.basename(pvol)
      call(["vol2bird",localfile],stdout=myfile)
   myfile.close()
   copyfile(myfile,os.path.basename(cwd)+"test.txt")

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

