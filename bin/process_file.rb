#!/usr/bin/env ruby

require 'aws-sdk'

Aws.config.update({
  region: 'us-east-1',
})

data_file = ENV['DATAFILE']
output_bucket = ENV['DEST_BUCKET']
output_file = File.basename(data_file)
s3 = Aws::S3::Client.new

if data_file.include?(".gz")

  gz = Zlib::GzipReader.new(s3.get_object(bucket: 'noaa-nexrad-level2', key: data_file).body)
  File.open('data', 'w') { |file| file.write(gz.read) }
  gz.close
else
  resp = s3.get_object(
   response_target: 'data',
   bucket: 'noaa-nexrad-level2',
   key: data_file)
end

`vol2bird data output`

# upload file from disk in a single request, may not exceed 5GB
File.open('output', 'rb') do |file|
  s3.put_object(bucket: output_bucket, key: "#{output_file}.h5", body: file)
end
