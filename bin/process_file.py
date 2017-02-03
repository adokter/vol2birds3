import boto
import os
from subprocess import call

data_file = os.environ["DATAFILE"]
output_bucket = os.environ["DEST_BUCKET"]
localfile = os.path.basename(data_file)
output_file=localfile+'.h5'

s3 = boto.connect_s3()

bucket = s3.get_bucket('noaa-nexrad-level2')
s3key = bucket.get_key(data_file)
s3key.get_contents_to_filename(localfile)

call(["vol2bird",localfile,output_file])
