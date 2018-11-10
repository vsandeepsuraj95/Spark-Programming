from __future__ import print_function


from pyspark import SparkContext
from pyspark import SparkConf

conf = SparkConf().setMaster("local").setAppName("sample")
spark = SparkContext(conf=conf)

review = spark.textFile("review.csv").map(lambda line: line.split("::")).map(lambda x: (x[2], x[3])).combineByKey(lambda value: (float(value), 1),
                                                                                                                lambda x, value: (float(x[0]) + float(value), x[1] + 1),
                                                                                                                lambda x, y: (x[0] + y[0], x[1] + y[1]))
top_average = review.map(lambda x: (x[0],x[1][0]/x[1][1])).top(10, key = lambda x: x[1])


business = spark.textFile("business.csv").map(lambda line: line.split("::")).map(lambda x:(x[0],(x[1],x[2]))).distinct()

ans = spark.parallelize(top_average).map(lambda x:(x[0], x[1])).join(business).map(lambda x: (x[0], x[1][1][0], x[1][1][1], x[1][0]))

ans.repartition(1).saveAsTextFile("q4a")
