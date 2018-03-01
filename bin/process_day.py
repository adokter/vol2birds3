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
from boto.s3.key import Key

def main(argv):
   me = sys.argv[0] 

   radar=''
   date = ''
   night = False
   day = False
   step = 5
   zipQ = False
   aws = False
   docker = False
   clut = False
   config = ''
   bucket = 'vol2bird'
   prefix = ''

   try:
      opts, args = getopt.getopt(argv,"hanDgcr:d:s:o:b:p:",["help","aws","night","day","gzip","clut","radar=","date=","step=","opts=","bucket=","prefix="])
   except getopt.GetoptError:
      print "error: unrecognised arguments"
      printSyntax(me)
      sys.exit(2)
   for opt, arg in opts:
      if opt in ('-h', "--help"):
         printSyntax(me)
         printOptions()
         sys.exit()
      elif opt in ("-n", "--night"):
         night = True
      elif opt in ("-D", "--day"):
         day = True
      elif opt in ("-a", "--aws"):
         aws = True
      elif opt in ("-g", "--gzip"):
         zipQ = True
      elif opt in ("-c", "--clut"):
         clut = True
      elif opt in ("-d", "--date"):
         date = arg
      elif opt in ("-r", "--radar"):
         radar = arg
      elif opt in ("-s", "--step"):
         step = float(arg)
      elif opt in ("-o","--opts"):
         config = arg
      elif opt in ("-b","--bucket"):
         bucket = arg
      elif opt in ("-p","--prefix"):
         prefix = arg
   if not(radar != '' and date != ''):
      print "error: both a radar and date specification required"
      printSyntax(me)
      sys.exit()

   # check whether we are inside a Docker container
   fcgroup="/proc/1/cgroup"
   if os.path.exists(fcgroup):
      with open(fcgroup, "r") as cgroupfile:
         if "docker" in cgroupfile.read():
             docker=True

   # store the current working directory
   cwd = os.getcwd()

   # make a temporary directory to store the radar data to be downloaded
   tmppath=tempfile.mkdtemp()
   # change the working directory to the new temporary directory
   os.chdir(tmppath)

   # copy the radar files, using radcp with arguments split as a list
   # keep only the radcp arguements
   argscp=argv
   if '--gzip' in argscp:
      argscp.remove('--gzip')
   if '-g' in argscp:
      argscp.remove('-a')
   if '--aws' in argscp:
      argscp.remove('--aws')
   if '-a' in argscp:
      argscp.remove('-a')
   if '--opts' in argscp:
      argscp.remove('--opts')
   if '-o' in argscp:
      argscp.remove('-o')
   if '--clut' in argscp:
      argscp.remove('--clut')
   if '-c' in argscp:
      argscp.remove('-c')
   if '--bucket' in argscp:
      argscp.remove('--bucket')
   if '-b' in argscp:
      argscp.remove('-b')
   if '--prefix' in argscp:
      argscp.remove('--prefix')
   if '-p' in argscp:
      argscp.remove('-p')
   radcp.main(argscp)

   # count the number of files
   NFile = len(os.listdir(tmppath))
   if NFile == 0:
      print >> sys.stderr, 'no files found'
      shutil.rmtree(tmppath)
      sys.exit()

   # get the file list of polar volumes to be processed
   pvols = sorted(os.listdir(tmppath))

   # write an option file if opts argument is set
   # should contain option.conf statements separated by \n
   if config != '':
      optsfile = open("options.conf", "w")
      optsfile.write(config.replace('\\n', '\n'))
      optsfile.close()

   # append cluttermap filename to options.conf
   if docker:
      if clut:
         if os.path.exists("options.conf"):
            mode="a"
         else:
            mode="w"
         optsfile =  open("options.conf", mode)
         optsfile.write("\nUSE_CLUTTERMAP=TRUE\nCLUTTERMAP=/opt/occult/"+radar+".h5")
         optsfile.close()

   # construct output filename from input argument string
   fout=radar+date+".txt"
   fout=fout.replace('/','')

   with open(fout, "a") as myfile:
      mynull = open('/dev/null', 'w')
      for pvol in pvols:
         localfile = os.path.basename(pvol)
         # process the volume file with vol2bird, write to myfile
         call(["vol2bird",localfile],stdout=myfile,stderr=mynull)
      myfile.close()
      mynull.close()
      # compress myfile and copy it to original working directory
      if zipQ:
         with open(fout, "r") as myfile:
            with gzip.open(fout + ".gz", 'wb') as zipfile:
               shutil.copyfileobj(myfile, zipfile)
               zipfile.close()
               # include gz extension to the output file
               fout=fout+".gz"
            myfile.close()


   # upload the output to s3
   if aws:
      conn = boto.connect_s3()
      bucket = conn.get_bucket(bucket)
      k = Key(bucket)
      k.key = prefix+"/"+radar+"/"+date+"/"+fout 
      k.set_contents_from_filename(fout)
   else:
      if docker:
         copyfile(fout,'/data/'+fout)
      else:
         copyfile(fout,cwd+'/'+fout)

   # clean up 
   shutil.rmtree(tmppath)

def printSyntax(me):
    print 'Usage: '
    print '  '+me+' -r <radar> -d <date> [--step <mins>] [--opts <options>] [--bucket <bucket>] [--prefix <prefix>] [--night] [--day] [--gzip] [--aws] [--clut]'
    print '  '+me+' -h | --help'

def printOptions():
    print '\nOptions:'
    print '  -h --help     Show this screen'
    print '  -r --radar    Specify NEXRAD radar, e.g. KBGM'
    print '  -d --date     Specify date in yyyy/mm/dd format'
    print '  -s --step     Minimum timestep in minutes between consecutive polar volumes [default: 5]'
    print '  -o --opts     The lines to write to the options.conf configuration file, separate lines by "\\n"'
    print '  -b --bucket   The AWS bucket name where profiles will be stored [default: vol2bird]'
    print '  -p --prefix   The AWS prefix (i.e. bucket postfix) to add to the filename for storing profiles'
    print '  -n --night    If set, only download nighttime data'
    print '  -d --day      If set, only download daytime data'
    print '  -g --gzip     Compress output'
    print '  -a --aws      Store output in bucket on aws'
    print '  -c --clut     Use cluttermap [docker container option only, ignored otherwise]'
 
if __name__ == "__main__":
   main(sys.argv[1:])
