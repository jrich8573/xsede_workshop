#!/bin/pyspark 

from pyspark import SparkConf, SparkContext
from pyspark.mllib.clustering import KMeans

conf = SparkConf().setMaster("local").setAppName("Test App")
sc = SparkContext(conf=conf)

rdd1 = sc.textFile("5000_points.txt")

rdd2 = rdd1.map(lambda x: x.split())
rdd3 = rdd2.map(lambda x: [int(x[0]), int(x[1])])

# 




for trails in range(10):
        for clusters in range(12, 18):
                model = KMeans.train(rdd3, clusters)
                cost = model.computeCost(rdd3)
                centers = model.clusterCenters
                if cost<1e+13:
                        print clusters, cost
                        for coords in centers:
                                print int(coords[0]), int(coords[1])
                        break

