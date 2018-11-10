from __future__ import print_function

import sys
from pyspark import SparkContext
from pyspark import SparkConf
from operator import add

from pyspark.sql import SparkSession
import pyspark.sql.functions as f


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)

    conf = SparkConf().setMaster("local").setAppName("sample")
    sc = SparkContext(conf=conf)

#spark
    lines = sc.textFile('words.txt')
    counts = lines.flatMap(lambda x: x.split(' ')) \
    .map(lambda x: (x, 1)) \
    .reduceByKey(add)
    output = counts.collect()
    for (word, count) in output:
        print("%s: %i" % (word, count))



    #spark sql
    #df = spark.read.text('words.txt')
    #df.show()
    #df.withColumn('word', f.explode(f.split(df['value'], ' '))).groupBy('word').count().show()
    #spark.stop()