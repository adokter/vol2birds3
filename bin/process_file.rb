#!/usr/bin/env ruby

require 'aws-sdk'

Aws.config.update({
  region: 'us-east-1',
})

data_file = ENV['DATAFILE']
output_bucket = ENV['DEST_BUCKET']
output_file = File.basename("#{data_file}")

# this command takes 3 secs on my local host, but is probably faster on AWS:
s3 = Aws::S3::Client.new

resp = s3.get_object(response_target: "#{output_file}",bucket: 'noaa-nexrad-level2',key: data_file)

`vol2bird #{output_file} output`

# upload file from disk in a single request, may not exceed 5GB
File.open('output', 'rb') do |file|
  s3.put_object(bucket: output_bucket, key: "#{output_file}.h5", body: file)
end
