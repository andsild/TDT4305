import unittest
import logging, os, time, sys
try:
    sys.path.append(os.path.join(
        os.environ['SPARK_HOME'], "python"))
    sys.path.append(os.path.join(os.environ['SPARK_HOME'], "python", "lib",
                                    "py4j-0.8.2.1-src.zip"))
except KeyError:
    print("SPARK_HOME not set")
    sys.exit(1)
from pyspark import SparkContext
from pyspark import SparkConf

from utility import *
from foursquare.foursquare_large import *
from foursquare.task3 import *

class TaskTestsComplete(unittest.TestCase):
    # TODO: overwrite and insert your URL here
    # see also "runtasks.sh" for how to run programs (if you want to)
    CITYFILE_URL="hdfs://localhost:8020/user/andesil/dataset_TIST2015_Cities.txt" 
    FOURSQUARE_URL="hdfs://localhost:8020/user/andesil/dataset_TIST2015.tsv" 

    def setUp(self):
        self.sc = SparkContext()

        self.rddCheckins = self.sc.textFile(self.FOURSQUARE_URL) \
            .map(lambda line: line.split('\t'))
        self.rddCities = self.sc.textFile(self.CITYFILE_URL) \
            .map(lambda line: line.split('\t'))
        
# filter the header pre-emptively
        header = self.rddCheckins.first()
        self.rddCheckins = self.rddCheckins.filter(lambda x: x != header)

# cleanup previous runs
        task_output_dirs = {2: "task2foursquareout",
                            3: "task3output",
                            "4b": "task4output",
                            6: "task6output",
                            7: "task7output",
                            }
        for dirname in task_output_dirs.values():
            if path.exists(dirname):
                shutil.rmtree(dirname)

        self.task_output_dirs = task_output_dirs

        print "beginning ..."
        self.start = time.time()

    def tearDown(self):
        end = (time.time() - self.start)
        print "finished attempt in ", end, " seconds"
        
        self.rddCheckins.unpersist()
        self.rddCities.unpersist()

        self.sc.stop()

    def testTask2(self):
        """ fastest approach
        """
        result = calculateLocalTime(self.rddCheckins, 3, 4, self.task_output_dirs[2])

    def testTask2Cached(self):
        cachedFile = self.rddCheckins.cache()
        result = calculateLocalTime(cachedFile, 3, 4, self.task_output_dirs[2])

    def testTask3(self):
        """ fastest approach
        """
        citiesAsList =  self.rddCities \
            .map(lambda x: (x[0], x[4], float(x[1]), float(x[2]))).collect()
        res = findNearestCountryCity(citiesAsList, self.rddCheckins, 5, 6, self.task_output_dirs[3])

    def testTask3Cached(self):
        citiesAsList =  self.rddCities \
            .map(lambda x: (x[0], x[4], float(x[1]), float(x[2]))).collect()
        cachedFile = self.rddCheckins.cache()
        res = findNearestCountryCity(citiesAsList, cachedFile, 5, 6, self.task_output_dirs[3])

    def testTask3CachedBroadcast(self):
        citiesAsList =  self.sc.broadcast(self.rddCities \
            .map(lambda x: (x[0], x[4], float(x[1]), float(x[2]))).collect())
        cachedFile = self.rddCheckins.cache()
        res = findNearestCountryCityBroadCasted(citiesAsList, cachedFile, 5, 6, self.task_output_dirs[3])

    def testTask3CachedBroadcastKDTree(self):
        """ does not work :( 
            (I do not know how to make scipy's structure work with RDD's)
        """
        from numpy import array
        from scipy import spatial
        citiesAsList =  self.sc.broadcast(self.rddCities \
            .map(lambda x: [x[0], x[4]]).collect())
        citiesAsArray = array(self.rddCities \
            .map(lambda x: (float(x[1]), float(x[2]))).collect())
        citiesAsTree = spatial.KDTree(citiesAsArray)
        res = findNearestCountryCityKDTree(citiesAsList, citiesAsTree, self.rddCheckins, 5, 6, self.task_output_dirs[3])


    def testTask4a(self):
        res = findUniqueUsers(self.rddCheckins, 1)
        print "Answer task4a COUNTED: ", res

    def testTask4b(self):
        res = findUniqueUsersAndSaveToDisk(self.rddCheckins, 2, self.task_output_dirs["4b"]) 

    def testTask4c(self):
        res = findUniqueUsersDistinct(self.rddCheckins, 2)

    def testTask4d(self):
        citiesAsList =  self.rddCities \
            .map(lambda x: (x[0], x[4], float(x[1]), float(x[2]))).collect()
        res = findCountries(citiesAsList, self.rddCheckins, 5, 6, self.task_output_dirs[3])
        print "Result is :", res

    def testTask4e(self):
        citiesAsList =  self.rddCities \
            .map(lambda x: (x[0], x[4], float(x[1]), float(x[2]))).collect()
        res = findCities(citiesAsList, self.rddCheckins, 5, 6, self.task_output_dirs[3])
        print "Result is :", res

    def testTask5(self):
        res = histogramNumberOfCheckins(self.rddCheckins, 2)

    def testTask6(self):
        """ this is the one I ended up using
        """
        res = distanceTraveledInOneCheckinOnePass(self.sc, self.rddCheckins, 4, self.task_output_dirs[6])

    def testTask6Cached(self):
        cached = self.rddCheckins.cache()
        print "cache done"
        res = distanceTraveledInOneCheckin(self.sc, cached, 4, self.task_output_dirs[6])

    def testTask6Subtracted(self):
        res = distanceTraveledInOneCheckinSubtracted(self.sc, self.rddCheckins, 4, self.task_output_dirs[6])

    def testTask6SubtractedCached(self):
        cached = self.rddCheckins.cache()
        print "cache done"
        res = distanceTraveledInOneCheckinSubtracted(self.sc, cached, 4, self.task_output_dirs[6])


    def testTask6AggregateCached(self):
        cached = self.rddCheckins.cache()
        print "cache done"
        res = distanceTraveledInOneCheckinOnePass(self.sc, cached, 4, self.task_output_dirs[6])
    
    def testTask7a(self):
        res = findLongestSessions(self.sc, self.rddCheckins, 50, 100, self.task_output_dirs[7])
