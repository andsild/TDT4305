def quiet_logs( sc ):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel( logger.Level.WARN )
  logger.LogManager.getLogger("akka").setLevel( logger.Level.WARN )

from math import *
def calculateHaversine(lat1, lon1, lat2, lon2):
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return c * r

def calculateHaversinePoints(p1, p2):
    return Point(p2.lat, p2.lon, p1.distance + calculateHaversine(p1.lat, p1.lon, p2.lat, p2.lon))

class Point(object):
    def __init__(self, lat,lon, distance):
        self.lat = float(lat)
        self.lon = float(lon)
        self.distance = distance

from datetime import timedelta, datetime
from dateutil.parser import parse
from time import strptime
def getTime(timeAsString, offsetInMinutes):
    dateFormat = "%Y-%m-%d %H:%M:%S"
    time = strptime(timeAsString, dateFormat)
    date = datetime(*time[0:6])
    ret = date + timedelta(minutes=int(offsetInMinutes))
    return str(ret)
