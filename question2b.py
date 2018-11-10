from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import udf, desc
from pyspark.sql.types import IntegerType
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("sql_sample")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)


#For each user A and list of briends [B, C, D] function returns (A, B, B) (A, B, C) (A, B, D), (A, C, B) (A, C, C) (A, C, D) etc
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
                a.append((b[0],b[1], f))
        return a
    else:
        z=[]
        z.append((x,"",""))
        return z


def getline(x):
    if len(x.split("\t")) == 2:
        return x.split('\t')[0], x.split('\t')[1]
    else:
        return x.split('\t')[0], ""


lines = sc.textFile('soc-LiveJournal.txt')
mutual = lines.map(lambda x: (getline(x))).flatMap(lambda x: generatekey(x[0], x[1])).toDF()
mutual = mutual.groupBy(mutual._1, mutual._2, mutual._3).count().withColumnRenamed('count','n')

mutual = mutual.where(mutual['n']>1)

top = mutual.groupBy(mutual._1, mutual._2).count().withColumnRenamed('count', 'n').orderBy(desc('n')).limit(10)#userA, userB, count
top.cache()
user = sc.textFile('userdata.txt').map(lambda x: x.split(',')).toDF()
usera = user.select(user._1.alias('A'),user._2.alias('A_fn'),user._3.alias('A_ln'),user._4.alias('A_add'))
userb = user.select(user._1.alias('B'),user._2.alias('B_fn'),user._3.alias('B_ln'),user._4.alias('B_add'))

top.show()
ans = top.join(usera, top._1 == usera.A)
ans.cache()
ans.show()
ans1 = ans.join(userb, ans._2 == userb.B)
ans1.cache()
ans1.show()
ans1.rdd.map(list).repartition(1).saveAsTextFile("q2b")