import sys
import boto3
import os
import shutil
import gzip
import tempfile
import getopt
from subprocess import call
from shutil import copyfile
from boto3.s3.key import Key

def main(argv):
   me = sys.argv[0] 

   clut = False
   config = ''
   key = ''
   prefix = ''
   source = 'noaa-nexrad-level2'
   bucket = 'is-birdcast-observed'
   aws = False

   try:
      opts, args = getopt.getopt(argv,"haco:k:b:p:",["help","aws","clut","opts=","key=","bucket=","prefix="])
   except getopt.GetoptError:
      print("error: unrecognised arguments")
      printSyntax(me)
      sys.exit(2)
   for opt, arg in opts:
      if opt in ('-h', "--help"):
         printSyntax(me)
         printOptions()
         sys.exit()
      elif opt in ("-c", "--clut"):
         clut = True
      elif opt in ("-k", "--key"):
         key = arg
      elif opt in ("-o","--opts"):
         config = arg
      elif opt in ("-s","--source"):
         source = arg
      elif opt in ("-b","--bucket"):
         bucket = arg
      elif opt in ("-p","--prefix"):
         prefix = arg
      elif opt in ("-a", "--aws"):
         aws = True
   if not(file != ''):
      print("error: file specification required")
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
   
   s3 = boto3.resource('s3')


   radar = os.path.basename(key)[0:4]
   date = os.path.basename(key)[4:12]
   datetime = datetime.strptime(os.path.basename(key)[4:19], '%Y%m%d_%H%M%S')
   localfile = os.path.basename(key)

   NFile = len(os.listdir(tmppath))

   s3.Bucket(source).download_file(key, localfile)


   if NFile == 0:
      sys.stderr.write("no downloaded file found")
      shutil.rmtree(tmppath)
      sys.exit()

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
   fout=localfile+".txt"
   h5file=localfile+".h5"

   with open(fout, "a") as myfile:
      mynull = open('/dev/null', 'w')
      # process the volume file with vol2bird, write to myfile
      call(["vol2bird",localfile],stdout=myfile,stderr=mynull)
      myfile.close()
      mynull.close()

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
    print('Usage: ')
    print('  '+me+' -r <radar> -d <date> [--step <mins>] [--opts <options>] [--bucket <bucket>] [--prefix <prefix>] [--night] [--day] [--gzip] [--aws] [--clut]')
    print('  '+me+' -h | --help')

def printOptions():
    print('\nOptions:')
    print('  -h --help     Show this screen')
    print('  -k --key      Key in noaa-nexrad-level2 bucket of file to process')
    print('  -o --opts     The lines to write to the options.conf configuration file, separate lines by "\\n"')
    print('  -b --bucket   The AWS bucket name where profiles will be stored [default: vol2bird]')
    print('  -p --prefix   The AWS prefix (i.e. bucket postfix) to add to the filename for storing profiles')
    print('  -c --clut     Use cluttermap [docker container option only, ignored otherwise]')
    print('  -a --aws      Store output in bucket on aws')
 
if __name__ == "__main__":
   main(sys.argv[1:])
