from pyspark.sql import SparkSession
from pyspark.sql.functions import split
from pyspark.sql.functions import udf, desc
from pyspark.sql.types import IntegerType
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("sql_sample")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

review = sc.textFile("review.csv").map(lambda line: line.split("::")).map(lambda x: (x[0], x[1], x[2], float(x[3]))).toDF()
review = review.select(review._1.alias('review_id'), review._2.alias('user_id'), review._3.alias('business_id'), review._4.alias('stars'))

review = review.select('business_id', 'stars').groupBy('business_id').avg('stars')
review.cache()
review = review.orderBy(desc('avg(stars)')).limit(10)

business = sc.textFile("business.csv").map(lambda line: line.split("::")).toDF()
business = business.select(business._1.alias('business_id'), business._2.alias('address'), business._3.alias('category'))
ans = review.join(business, "business_id").distinct()

ans.cache()
ans.show()
ans.rdd.map(list).repartition(1).saveAsTextFile("q4b")
