from __future__ import print_function

import re
from pyspark import SparkContext
from pyspark import SparkConf


#For each user A and list of briends [B, C, D] function returns ([A, B] [B, C, D]), ([A, C] [B, C, D]), ([A, D][B, C, D])
def generatekey(x, y):
    if(y!=""):
        friends=y.split(",")
        a = []
        for f in friends:
            b = []
            b.append(int(x))
            b.append(int(f))
            b.sort()
            a.append((str(b), y))
        return a
    else:
        z=[]
        z.append((x,""))
        return z


#a=[A, B, C, D]; b=[C, D, E, F]; output=[C, D]
def intersection(a, b):
    x = a.split(",")
    y = b.split(",")
    if len(x)!=0 and len(y)!=0:
        c = []
        for u in x:
            for v in y:
                if v == u:
                    c.append(u)
        return str(c)
    else:
        return ""


def getline(x):
    if len(x.split("\t")) == 2:
        return x.split('\t')[0], x.split('\t')[1]
    else:
        return x.split('\t')[0], ""


conf = SparkConf().setMaster("local").setAppName("sample")
spark = SparkContext(conf=conf)

lines = spark.textFile('soc-LiveJournal.txt')
mutual = lines.map(lambda x: (getline(x))).flatMap(lambda x: generatekey(x[0], x[1]))\
    .reduceByKey(lambda a, b: intersection(a, b))\
    .filter(lambda x: re.match(r"\[.+\]", x[1]) is not None)#remove user pairs that have no mutual friends
mutual.saveAsTextFile("q1a")

