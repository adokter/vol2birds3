#!/usr/bin/python

import sys, getopt
import datetime, astral
from datetime import datetime

def main(argv):
   latitude = ''
   longitude = ''
   date = ''
   try:
      opts, args = getopt.getopt(argv,"hx:y:d:",["lon=","lat=","date="])
   except getopt.GetoptError:
      print 'suntime.py -y <lat> -x <lon> -d <date>'
      sys.exit(2)
   for opt, arg in opts:
      if opt == '-h':
         print 'suntime.py -y <lat> -x <lon> -d <date>'
         sys.exit()
      elif opt in ("-y", "--lat"):
         latitude = arg
      elif opt in ("-x", "--lon"):
         longitude = arg
      elif opt in ("-d", "--date"):
         date = arg
   if not(latitude.isdigit() and longitude.isdigit() and date.isdigit()):
      print 'suntime.py -y <lat> -x <lon> -d <date>'
      sys.exit()

   latitude=float(latitude)
   longitude=float(longitude)
   datetime_object = datetime.strptime(date, '%Y%m%d')
   print 'Date is ', str(datetime_object)
   print 'Latitude is ', latitude
   print 'Longitude is ', longitude

   l=astral.Location()
   l.timezone='UTC'
   l.longitude=longitude
   l.latitude=latitude
   sun=l.sun(date=datetime_object)
   print('Sunrise: %s' % str(sun['sunrise']))
   print(sun['sunrise'].strftime('%Y%m%d_%H%M%S'))

if __name__ == "__main__":
   main(sys.argv[1:])
