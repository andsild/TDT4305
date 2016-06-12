from operator import add
from os import path, system
import shutil

from utility import calculateHaversine, calculateHaversinePoints, Point, getTime

from task3 import findNearestCountryCity


def calculateLocalTime(rddCheckins, indexOfUtcTime1, indexOfOffsett, outfile):
    """ Task 2 """
    rddLocalTime = rddCheckins \
        .map(lambda row: getTime(row[indexOfUtcTime1], row[indexOfOffsett]))
    rddLocalTime.saveAsTextFile(outfile)
    return rddLocalTime

def findUniqueUsers(rddCheckins, index1):
    """ Task 4a """
    user_id_count = rddCheckins \
        .map(lambda x: x[index1]) \
        .countByValue()
    numberUsers = len(user_id_count)
    return numberUsers 


def findUniqueUsersAndSaveToDisk(rddCheckins, index1, outfile):
    """ task 4b
        I don't do a countByKey since I want to save to disk afterward 
        (countByKey returns dict)
    """
    user_id_count = rddCheckins \
        .map(lambda x: (x[index1], 1)) \
        .reduceByKey(add)

    user_id_count.saveAsTextFile(outfile)

    return user_id_count 

def findUniqueUsersDistinct(rddCheckins, index1):
    """  Task 4c
    here I don't need to filter out the header, but it is done anyway (ease of programming)
    """
    user_id_count = rddCheckins \
        .map(lambda x: x[index1]) \
        .distinct() \
        .count()
    return user_id_count 


def findCountries(citiesAsList, rddCheckins, index1, index2, outfile):
    """ task 4d
    """
# FIXME: THE COUNT FROM THIS FUNCTION IS *WRONG* (not sure why)
    res = findNearestCountryCity(citiesAsList, rddCheckins, index1, index2, outfile, False)
    return res.map(lambda x: x[1][-1]).distinct().count()

def findCities(citiesAsList, rddCheckins, index1, index2, outfile):
    """ task 4e
    """
# FIXME: THE COUNT FROM THIS FUNCTION IS *WRONG* (not sure why)
    res = findNearestCountryCity(citiesAsList, rddCheckins, index1, index2, outfile, False)
    return res.map(lambda x: x[1][-2]).distinct().count()



def histogramNumberOfCheckins(rddCheckins, index1):
    """ task 5
    """
    import matplotlib.pyplot as plt
    import plotly.plotly as py    
    import numpy
    histogram=plt.figure()

# we dont use countbykey, etc, because they return dictionaries

    number_of_checkins_per_session = rddCheckins \
        .map(lambda x: (x[2], 1)) \
        .reduceByKey(add) \
        .map(lambda x: x[1])

    buckets, data = number_of_checkins_per_session.histogram(range(0, 600))
    """ filter out the 0 entry and trailing 0's (no checkins have zero occurences)
        why not pass in range(1, 600) instead? 
        Because then no entry ends up in the ``1'' entry
    """
    highest_occurence = max(index for index,num in enumerate(data) if num > 0)+1
    buckets = buckets[1:highest_occurence]
    data = data[1:highest_occurence]

    plt.bar(buckets, data)
    plt.ylabel("No. of users")
    plt.xlabel("No. of checkins")
    plt.title("Histogram of No. of  sessions in foursquare data")
    plt.xscale('log')
    plt.yscale('log')
    """ Round up to nearest million """
    plt.axis([1, highest_occurence+1, 1, (((max(data)/1000000)+1)*1000000)])
    plt.show()
    # py.plot_mpl(histogram, filename="test.out")
    return 1


def distanceTraveledInOneCheckin(sc, rddCheckins, how_many_checkins, outfile):
    """ task 6 (not the fastest solution)
    """
    number_of_checkins_per_session = sc.broadcast(set( \
        rddCheckins \
            .map(lambda x: (x[2], 1)) \
            .reduceByKey(add) \
            .filter(lambda x: x[0] > how_many_checkins)
            .map(lambda x: x[0])
            .collect()))

    pointsOfInterest = rddCheckins \
        .filter(lambda x: x[2] in number_of_checkins_per_session.value) \
        .map(lambda x: (x[2], Point(x[5], x[6], 0))) \
        .reduceByKey(calculateHaversinePoints) \
        .map(lambda x: (x[0], x[1].distance))

    pointsOfInterest.saveAsTextFile(outfile)
    return pointsOfInterest


def distanceTraveledInOneCheckinSubtracted(sc, rddCheckins, how_many_checkins, outfile):
    """ task 6 (not the fastest solution
    """
    number_of_checkins_per_session = rddCheckins \
            .map(lambda x: (x[2], 1)) \
            .reduceByKey(add) \
            .filter(lambda x: x[1] < how_many_checkins) \
            .map(lambda x: (x[0], 0))  # has to be a list, 0 is not used

    pointsOfInterest = rddCheckins \
        .map(lambda x: (x[2], Point(x[5], x[6], 0))) \
        .subtractByKey(number_of_checkins_per_session) \
        .reduceByKey(calculateHaversinePoints) \
        .map(lambda x: (x[0], x[1].distance))

    pointsOfInterest.saveAsTextFile(outfile)
    return pointsOfInterest


def distanceTraveledInOneCheckinOnePass(sc, rddCheckins, how_many_checkins, outfile):
    """ task 6 (fastest)
    """
    seqOp = lambda acc,val: [val[0], val[1], acc[2] + val[2]]
    combineOp = lambda accLeft, accRight:  [accLeft[0], accLeft[1], accLeft[2] + accRight[2]]

    number_of_checkins_per_session = rddCheckins \
            .map(lambda x: [x[2], (x[5], x[6], 1)]) \
            .aggregateByKey(("", "", 0), seqOp, combineOp) \
            .filter(lambda x: x[1][2] > how_many_checkins) \
            .map(lambda x: (x[0], Point(x[1][0], x[1][1], 0))) \
            .reduceByKey(calculateHaversinePoints) \
            .map(lambda x: (x[0], x[1].distance)) # session id and distance

    number_of_checkins_per_session.saveAsTextFile(outfile)

    return number_of_checkins_per_session

def calculateHaversinePointsTask7(inp1, inp2):
    """ Specific utility function for task 7
    """
    p1, p2 = inp1[0], inp2[0]
    return (Point(p2.lat, p2.lon, p1.distance + calculateHaversine(p1.lat, p1.lon, p2.lat, p2.lon)),  inp2[1:])

def findLongestSessions(sc, rddCheckins, mininum_distance, max_count, outfile):
    """ task 7
    """
    mininum_distance -= 0.0001 # to do greater than filtering

    pointsOfInterest = [row[0] for row in \
        rddCheckins \
        .map(lambda x: (x[2], Point(x[5], x[6], 0))) \
        .reduceByKey(calculateHaversinePoints) \
        .filter(lambda x: x[1].distance > mininum_distance) \
        .takeOrdered(max_count, lambda x: -x[1].distance)]

    checkins = sc.broadcast(set(pointsOfInterest))

    # Map: output data as csv
    data = rddCheckins.filter(lambda x: x[2] in checkins.value) \
        .map(lambda x: (';'.join(x[:3]) + ";" +  getTime(x[3], x[4]) + ';' + ';'.join(x[5:]))) \
        .coalesce(1)

    data.saveAsTextFile(outfile)

    header = "checkin_id\\;user_id\\;session_id\\;date\\;lat\\;lon\\;category\\;subcategory"
    system("(echo " + header + " && cat " + outfile + "/part-00000 ) >> " + outfile + "/out.csv")

    return data


