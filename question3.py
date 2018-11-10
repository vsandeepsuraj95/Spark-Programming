from __future__ import print_function

from pyspark import SparkContext
from pyspark import SparkConf

conf = SparkConf().setMaster("local").setAppName("sample")
spark = SparkContext(conf=conf)

review = spark.textFile("review.csv").map(lambda line: line.split("::")).map(lambda x: (x[2], (x[1],x[3])))

business = spark.textFile("business.csv").map(lambda line: line.split("::")).filter(lambda x: "Stanford" in x[1]).map(lambda x:(x[0],(x[1],x[2]))).distinct()

ans= review.join(business).map(lambda x:(x[1][0],x[0]))
ans.repartition(1).saveAsTextFile("q3a")
