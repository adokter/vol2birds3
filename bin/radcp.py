#!/usr/bin/python

import sys, getopt
import datetime, pytz, astral
from datetime import datetime
import boto
import os
from subprocess import call
from datetime import timedelta

NEXRADlat={'KABR': 45.4558333333333, 'KABX': 35.1497222222222, 'KAKQ': 36.9838888888889, 'KAMA': 35.2333333333333, 'KAMX': 25.6111111111111, 'KAPX': 44.9072222222222, 'KARX': 43.8227777777778, 'KATX': 48.1944444444444, 'KBBX': 39.4961111111111, 'KBGM': 42.1997222222222, 'KBHX': 40.4983333333333, 'KBIS': 46.7708333333333, 'KBLX': 45.8538888888889, 'KBMX': 33.1722222222222, 'KBOX': 41.9558333333333, 'KBRO': 25.9161111111111, 'KBUF': 42.9488888888889, 'KBYX': 24.5975, 'KCAE': 33.9486111111111, 'KCBW': 46.0394444444444, 'KCBX': 43.4908333333333, 'KCCX': 40.9230555555555, 'KCLE': 41.4130555555555, 'KCLX': 32.6555555555556, 'KCRP': 27.7841666666667, 'KCYS': 41.1519444444444, 'KDAX': 38.5011111111111, 'KDDC': 37.7608333333333, 'KDFX': 29.2727777777778, 'KDGX': 32.28, 'KDIX': 39.9469444444444, 'KDLH': 46.8369444444444, 'KDMX': 41.7313888888889, 'KDOX': 38.8255555555556, 'KDTX': 42.6997222222222, 'KDVN': 41.6116666666667, 'KDYX': 32.5383333333333, 'KEAX': 38.8102777777778, 'KEMX': 31.8936111111111, 'KENX': 42.5863888888889, 'KEOX': 31.4605555555556, 'KEPZ': 31.8730555555556, 'KESX': 35.7011111111111, 'KEVX': 30.5644444444444, 'KEWX': 29.7038888888889, 'KEYX': 35.0977777777778, 'KFCX': 37.0244444444444, 'KFDR': 34.3622222222222, 'KFDX': 34.6352777777778, 'KFFC': 33.3636111111111, 'KFSD': 43.5877777777778, 'KFSX': 34.5744444444444, 'KFTG': 39.7866666666667, 'KFWS': 32.5730555555556, 'KGGW': 48.2063888888889, 'KGJX': 39.0622222222222, 'KGLD': 39.3663888888889, 'KGRB': 44.4983333333333, 'KGRK': 30.7219444444444, 'KGRR': 42.8938888888889, 'KGSP': 34.8833333333333, 'KGWX': 33.8966666666667, 'KGYX': 43.8913888888889, 'KHDX': 33.0763888888889, 'KHGX': 29.4719444444444, 'KHNX': 36.3141666666667, 'KHPX': 36.7366666666667, 'KHTX': 34.9305555555556, 'KICT': 37.6547222222222, 'KICX': 37.5908333333333, 'KILN': 39.4202777777778, 'KIND': 39.7075, 'KINX': 36.175, 'KIWA': 33.2891666666667, 'KIWX': 41.3588888888889, 'KJAX': 30.4847222222222, 'KJGX': 32.675, 'KJKL': 37.5908333333333, 'KLBB': 33.6538888888889, 'KLCH': 30.1252777777778, 'KLGX': 47.1169444444444, 'KLIX': 30.3366666666667, 'KLNX': 41.9577777777778, 'KLOT': 41.6047222222222, 'KLRX': 40.7397222222222, 'KLSX': 38.6988888888889, 'KLTX': 33.9894444444444, 'KLVX': 37.9752777777778, 'KLWX': 38.9752777777778, 'KLZK': 34.8366666666667, 'KMAF': 31.9433333333333, 'KMAX': 42.0811111111111, 'KMBX': 48.3925, 'KMHX': 34.7761111111111, 'KMKX': 42.9677777777778, 'KMLB': 28.1133333333333, 'KMOB': 30.6794444444444, 'KMPX': 44.8488888888889, 'KMQT': 46.5311111111111, 'KMRX': 36.1686111111111, 'KMSX': 47.0411111111111, 'KMTX': 41.2627777777778, 'KMUX': 37.155, 'KMVX': 47.5277777777778, 'KNKX': 32.9188888888889, 'KNQA': 35.3447222222222, 'KOAX': 41.3202777777778, 'KOHX': 36.2472222222222, 'KOKX': 40.8655555555556, 'KOTX': 47.6802777777778, 'KPAH': 37.0683333333333, 'KPBZ': 40.5316666666667, 'KPDT': 45.6905555555556, 'KPOE': 31.1555555555556, 'KPUX': 38.4594444444445, 'KRAX': 35.6655555555556, 'KRGX': 39.7552777777778, 'KRIW': 43.0661111111111, 'KRLX': 38.3111111111111, 'KRTX': 45.7147222222222, 'KSFX': 43.1058333333333, 'KSGF': 37.2352777777778, 'KSHV': 32.4508333333333, 'KSJT': 31.3713888888889, 'KSOX': 33.8177777777778, 'KSRX': 35.2905555555556, 'KTBW': 27.7055555555556, 'KTFX': 47.4597222222222, 'KTLH': 30.3975, 'KTLX': 35.3330555555556, 'KTWX': 38.9969444444444, 'KUDX': 44.125, 'KUEX': 40.3208333333333, 'KVAX': 30.8902777777778, 'KVBX': 34.8380555555556, 'KVNX': 36.7408333333333, 'KVTX': 34.4116666666667, 'KVWX': 38.2602777777778, 'KYUX': 32.4952777777778, 'PABC': 60.7927777777778, 'PACG': 56.8527777777778, 'PAEC': 64.5113888888889, 'PAHG': 60.7258333333333, 'PAIH': 59.4619444444444, 'PAKC': 58.6794444444444, 'PAPD': 65.035, 'PGUA': 13.4544444444444, 'PHKI': 21.8941666666667, 'PHKM': 20.1255555555556, 'PHMO': 21.1327777777778, 'PHWA': 19.095, 'RKSG': 36.9594444444445, 'TJUA': 18.1155555555556, 'KCXX': 44.51111111111111, 'KILX': 40.15055555555555, 'KTYX': 43.755833333333335, 'KMXX': 32.53667}

NEXRADlon={'KABR': -98.4130555555556, 'KABX': -106.823888888889, 'KAKQ': -77.0072222222222, 'KAMA': -101.709166666667, 'KAMX': -80.4127777777778, 'KAPX': -84.7197222222222, 'KARX': -91.1911111111111, 'KATX': -122.495833333333, 'KBBX': -121.631666666667, 'KBGM': -75.9847222222222, 'KBHX': -124.291944444444, 'KBIS': -100.760555555556, 'KBLX': -108.606666666667, 'KBMX': -86.77, 'KBOX': -71.1369444444445, 'KBRO': -97.4188888888889, 'KBUF': -78.7366666666667, 'KBYX': -81.7030555555556, 'KCAE': -81.1183333333333, 'KCBW': -67.8066666666667, 'KCBX': -116.235555555556, 'KCCX': -78.0036111111111, 'KCLE': -81.8597222222222, 'KCLX': -81.0419444444444, 'KCRP': -97.5111111111111, 'KCYS': -104.806111111111, 'KDAX': -121.677777777778, 'KDDC': -99.9686111111111, 'KDFX': -100.280555555556, 'KDGX': -89.9844444444444, 'KDIX': -74.4108333333334, 'KDLH': -92.2097222222222, 'KDMX': -93.7227777777778, 'KDOX': -75.44, 'KDTX': -83.4716666666667, 'KDVN': -90.5808333333333, 'KDYX': -99.2541666666667, 'KEAX': -94.2644444444444, 'KEMX': -110.630277777778, 'KENX': -74.0638888888889, 'KEOX': -85.4594444444444, 'KEPZ': -106.698055555556, 'KESX': -114.891388888889, 'KEVX': -85.9213888888889, 'KEWX': -98.0283333333333, 'KEYX': -117.560833333333, 'KFCX': -80.2738888888889, 'KFDR': -98.9763888888889, 'KFDX': -103.63, 'KFFC': -84.5658333333333, 'KFSD': -96.7294444444444, 'KFSX': -111.197777777778, 'KFTG': -104.545833333333, 'KFWS': -97.3030555555556, 'KGGW': -106.625, 'KGJX': -108.213888888889, 'KGLD': -101.700555555556, 'KGRB': -88.1113888888889, 'KGRK': -97.3830555555555, 'KGRR': -85.5447222222222, 'KGSP': -82.22, 'KGWX': -88.3288888888889, 'KGYX': -70.2566666666666, 'KHDX': -106.122777777778, 'KHGX': -95.0791666666667, 'KHNX': -119.632222222222, 'KHPX': -87.285, 'KHTX': -86.0833333333333, 'KICT': -97.4427777777778, 'KICX': -112.862222222222, 'KILN': -83.8216666666667, 'KIND': -86.2802777777778, 'KINX': -95.5647222222222, 'KIWA': -111.67, 'KIWX': -85.6997222222222, 'KJAX': -81.7019444444444, 'KJGX': -83.3511111111111, 'KJKL': -83.3130555555556, 'KLBB': -101.814166666667, 'KLCH': -93.2158333333333, 'KLGX': -124.106666666667, 'KLIX': -89.8255555555556, 'KLNX': -100.576388888889, 'KLOT': -88.0847222222222, 'KLRX': -116.802777777778, 'KLSX': -90.6827777777778, 'KLTX': -78.4288888888889, 'KLVX': -85.9438888888889, 'KLWX': -77.4777777777778, 'KLZK': -92.2622222222222, 'KMAF': -102.189166666667, 'KMAX': -122.717222222222, 'KMBX': -100.865, 'KMHX': -76.8761111111111, 'KMKX': -88.5505555555555, 'KMLB': -80.6541666666667, 'KMOB': -88.2397222222222, 'KMPX': -93.5655555555555, 'KMQT': -87.5483333333333, 'KMRX': -83.4016666666667, 'KMSX': -113.986111111111, 'KMTX': -112.447777777778, 'KMUX': -121.898333333333, 'KMVX': -97.3255555555556, 'KNKX': -117.041944444444, 'KNQA': -89.8733333333333, 'KOAX': -96.3666666666667, 'KOHX': -86.5625, 'KOKX': -72.8638888888889, 'KOTX': -117.626666666667, 'KPAH': -88.7719444444444, 'KPBZ': -80.2183333333333, 'KPDT': -118.852777777778, 'KPOE': -92.9758333333334, 'KPUX': -104.181388888889, 'KRAX': -78.4897222222222, 'KRGX': -119.462222222222, 'KRIW': -108.477222222222, 'KRLX': -81.7230555555556, 'KRTX': -122.965555555556, 'KSFX': -112.686111111111, 'KSGF': -93.4005555555556, 'KSHV': -93.8413888888889, 'KSJT': -100.4925, 'KSOX': -117.635833333333, 'KSRX': -94.3616666666667, 'KTBW': -82.4016666666667, 'KTFX': -111.385277777778, 'KTLH': -84.3288888888889, 'KTLX': -97.2777777777778, 'KTWX': -96.2325, 'KUDX': -102.829722222222, 'KUEX': -98.4419444444444, 'KVAX': -83.0016666666667, 'KVBX': -120.395833333333, 'KVNX': -98.1277777777778, 'KVTX': -119.179722222222, 'KVWX': -87.7244444444444, 'KYUX': -114.656666666667, 'PABC': -161.874166666667, 'PACG': -135.529166666667, 'PAEC': -165.295, 'PAHG': -151.351388888889, 'PAIH': -146.301111111111, 'PAKC': -156.629444444444, 'PAPD': -147.501666666667, 'PGUA': 144.808333333333, 'PHKI': -159.552222222222, 'PHKM': -155.777777777778, 'PHMO': -157.18, 'PHWA': -155.568888888889, 'RKSG': 127.025555555556, 'TJUA': -66.0780555555556, 'KCXX': -73.16694444444445, 'KILX':-89.33694444444444, 'KTYX': -75.67999999999999, 'KMXX': -85.78972}

def main(argv):
   latitude = 10
   longitude = 10
   radar=''
   date = ''
   night = False
   midday = False
   step = 5
   utc=pytz.UTC
   dryrun=False
   try:
      opts, args = getopt.getopt(argv,"hnmr:d:s:",["night","midday","radar=","date=","step=","dryrun"])
   except getopt.GetoptError:
      print 'radcp.py -r <radar> -d <date> [--night] [--midday] [--step <mins>] [--dryrun]'
      print 'radcp.py -h | --help'
      sys.exit(2)
   for opt, arg in opts:
      if opt == '-h':
         print 'Usage: '
         print '  radcp.py -r <radar> -d <date> [--night] [--midday] [--step <mins>] [--dryrun]'
         print '  radcp.py -h | --help'
         print '\nOptions:'
         print '  -h --help     Show this screen'
         print '  -r --radar    Specify NEXRAD radar, e.g. KBGM'
         print '  -d --date     Specify date in yyyy/mm/dd format'
         print '  -n --night    If set, only download nighttime data'
         print '  -m --midday   If set, only download data file closest to noon'
         print '  -s --step     Downsampling timestep in minutes between consecutive polar volumes [default: 5]'
         print '  --dryrun      If set, do not download the data but list files selected for download'
         sys.exit()
      elif opt in ("-n", "--night"):
         night = True
      elif opt in ("-m", "--midday"):
         midday = True
      elif opt in ("-d", "--date"):
         date = arg
      elif opt in ("-r", "--radar"):
         radar = arg
      elif opt in ("-s", "--step"):
         step = int(arg)
      elif opt in ("--dryrun"):
         dryrun = True
   # not working yet ... validate(date)
   if not(radar != ''):
      print 'radcp.py -r <radar> -d <date> [--night] [--midday] [--step <mins>] [--dryrun]'
      print 'radcp.py -h | --help'
      sys.exit()

   latitude=NEXRADlat[radar]
   longitude=NEXRADlon[radar]

   datetime_object = utc.localize(datetime.strptime(date, '%Y/%m/%d'))
   datetime_prev = utc.localize(datetime.strptime('1900/01/01', '%Y/%m/%d'))
   # print '     Radar:', radar, 'Date:', datetime_object.strftime('%Y/%m/%d')
   # print '       Lat:', latitude,'Lon:', longitude
   # print 'Night only:', night

   l=astral.Location()
   l.timezone='UTC'
   l.longitude=longitude
   l.latitude=latitude
   sun=l.sun(date=datetime_object)
   sunprev=l.sun(date=datetime_object-timedelta(days=1))

   # print('   Sunrise: %s' % str(sun['sunrise']))
   # print('    Sunset: %s' % str(sun['sunset']))
   # print('PrevSunset: %s' % str(sunprev['sunset']))

   sunrise=sun['sunrise']
   if sun['sunrise'].day != sun['sunset'].day:
      sunset = sunprev['sunset']
   else:
      sunset = sun['sunset']
  
   daylength=sun['sunset']-sun['sunrise']
 
   if sunrise<sunset:
      noon=sunrise+daylength/2
   else:
      if (sunrise+daylength/2).day == sunset.day:
         noon=sunrise+daylength/2
      else:
         daylength=sunprev['sunset']-sunprev['sunrise']
         noon=sunrise-daylength/2

   data_dir=sunrise.strftime('%Y/%m/%d/'+radar)

   s3 = boto.connect_s3()

   bucket = s3.get_bucket('noaa-nexrad-level2')

   # prepare the datetimes of the radar files, and throw out non-radar files
   datetimes = [] 
   bucketlist = []
   for key in bucket.list(prefix=data_dir):
      fname = os.path.basename(key.name)
      if fname[0:4] != radar:
         continue
      datetime_key = utc.localize(datetime.strptime(fname[4:19], '%Y%m%d_%H%M%S'))
      datetimes.append(datetime_key)
      bucketlist.append(key)

   if len(bucketlist) == 0:
      sys.exit()
 
   # only keep the filenames that best fit the requested time grid
   delta = [x-datetime_object for x in datetimes]
   if midday:
      date_ref = [noon]
   else:
      date_ref = [datetime_object+timedelta(minutes=x) for x in range(0, 24*60, step)]
   index_ref = [getNearestIndex(datetimes,x) for x in date_ref]
   # delete duplicates, while preserving the time order of the files
   index_ref = unique(index_ref)
   bucketlist = [bucketlist[i] for i in index_ref]
   
   s3 = boto.connect_s3()

   for key in bucketlist:
      fname = os.path.basename(key.name)
      if fname[0:4] != radar:
         continue
      datetime_key = utc.localize(datetime.strptime(fname[4:19], '%Y%m%d_%H%M%S'))
      if night:
         if sunrise < sunset:
            if datetime_key > sunrise and datetime_key < sunset:
               continue
         else:
            if datetime_key < sunset or datetime_key > sunrise:
               continue

      datetime_prev=datetime_key

      # print or copy key
      if dryrun:
         print fname
      else:
         key.get_contents_to_filename(fname)

def validate(date_text):
    try:
        datetime.datetime.strptime(date_text, '%Y/%m/%d')
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY/MM/DD")

def getNearestIndex(mylist,value):
    deltas = [abs(value - x) for x in mylist]
    output=deltas.index(min(deltas))
    return output

def unique(seq):
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]

if __name__ == "__main__":
   main(sys.argv[1:])
