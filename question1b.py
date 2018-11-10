from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("sql_sample")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)


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
            for f in friends:
                a.append((str(b), f))
        return a
    else:
        z=[]
        z.append((x,""))
        return z


def getline(x):
    if len(x.split("\t")) == 2:
        return x.split('\t')[0], x.split('\t')[1]
    else:
        return x.split('\t')[0], ""


lines = sc.textFile('soc-LiveJournal.txt')
mutual = lines.map(lambda x: (getline(x))).flatMap(lambda x: generatekey(x[0], x[1])).toDF()#[A,B] C
mutual = mutual.groupBy(mutual._1, mutual._2).count().withColumnRenamed('count','n').cache()

mutual = mutual.where(mutual['n']>1)#if [A, B] C appears 2 times C is a mutual friend of A,B

mutual = mutual.groupBy(mutual._1).agg(f.collect_list(mutual._2))

mutual.cache()
mutual.show()

mutual.rdd.map(list).repartition(1).saveAsTextFile("q11b")
