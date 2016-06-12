from math import *
from utility import calculateHaversine
from scipy import spatial
import numpy as np
from ipdb import set_trace

def findNearestCountryCityKDTree(citiesAsList, citiesAsTree, rddCheckins, index1, index2, outpath):
    """  Task 3
    I cannot make the code work. The coordinates go into the KDtree, but
    I get error "AttributeError: 'leafnode' object has no attribute 'split_dim'"
    this only happens inside of spark - outside I can still make the same coordinates work.
    """
    distance_unit = float(111.045)
    radius = float(3000) # 2000 is too little
    ratio = radius / distance_unit 
    
    set_trace()
    def findNearestCountryCitySingleton(coordinates):
        _,index = citiesAsTree.query(coordinates, distance_upper_bound=500)
        return citiesAsList.value[index]

    ret = rddCheckins \
        .map(lambda x: (x[0], findNearestCountryCitySingleton((float(x[5]), float(x[6])))))

    ret.saveAsTextFile(outpath)
    return ret

def findNearestCountryCity(cities, rddCheckins, index1, index2, outfile, writeOut=True):
    """  Task 3 ( fastest approach )
    One degree of latitude = 111.045
    """
    distance_unit = float(111.045)
    radius = float(3000) # 2000 is too little
    ratio = radius / distance_unit 
    
    def findNearestCountryCitySingleton(input_lat, input_lon):
        latitude_lower = input_lat - ratio
        latitude_upper = input_lat + ratio
        longitude_lower = input_lat - \
            ratio / cos(radians(input_lat))
        longitude_upper = input_lat + \
            ratio / cos(radians(input_lat))
        outList = filter(lambda x: \
                    (x[2] > latitude_lower and x[2] < latitude_upper  and x[3] >  longitude_lower and x[3] < longitude_upper), cities)
        returnValue = map(lambda x: ((calculateHaversine(x[2], x[3],
                                input_lat, input_lon), x[0], x[1]) ),
                                outList)
        return sorted(returnValue)[0]

    ret = rddCheckins \
        .map(lambda x: (x[0], findNearestCountryCitySingleton(float(x[5]), float(x[6]))))

    if writeOut:
        ret.saveAsTextFile(outfile)
    return ret


def findNearestCountryCityBroadCasted(cities, rddCheckins, index1, index2, outfile):
    """  Task 3
    One degree of latitude = 111.045
    """

    distance_unit = float(111.045)
    radius = float(3000) # 2000 is too little
    ratio = radius / distance_unit 
    
    def findNearestCountryCitySingleton(input_lat, input_lon):
        latitude_lower = input_lat - ratio
        latitude_upper = input_lat + ratio
        longitude_lower = input_lat - \
            ratio / cos(radians(input_lat))
#(radius / (distance_unit * cos(radians(input_lat))))
        longitude_upper = input_lat + \
            ratio / cos(radians(input_lat))
             #(radius / (distance_unit * cos(radians(input_lat))))
        outList = filter(lambda x: \
                    (x[2] > latitude_lower and x[2] < latitude_upper  and x[3] >  longitude_lower and x[3] < longitude_upper), cities.value)
        # outList = cities
        returnValue = map(lambda x: ((calculateHaversine(x[2], x[3], input_lat, input_lon), x[0], x[1]) ), outList)
        return sorted(returnValue)[0]

# kd tree also, comparison
    ret = rddCheckins.filter(lambda x: x[5] != u"lat") \
        .map(lambda x: (x[0], findNearestCountryCitySingleton(float(x[5]), float(x[6]))))

    ret.saveAsTextFile(outfile)
    return ret
