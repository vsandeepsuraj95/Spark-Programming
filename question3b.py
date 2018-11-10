from pyspark.sql import SparkSession

from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("sql_sample")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

review = sc.textFile("review.csv").map(lambda line: line.split("::")).map(lambda x: (x[0], x[1], x[2], float(x[3]))).toDF()
review = review.select(review._1.alias('review_id'), review._2.alias('user_id'), review._3.alias('business_id'), review._4.alias('stars'))

business = sc.textFile("business.csv").map(lambda line: line.split("::")).toDF()
business = business.select(business._1.alias('business_id'), business._2.alias('address'), business._3.alias('category'))

business_s= business.select('business_id').where(business['address'].contains("Stanford"))
business_s.cache()
ans = review.join(business_s, "business_id").distinct().select('user_id', 'stars')
ans.cache()
ans.show()
ans.rdd.map(list).repartition(1).saveAsTextFile("q3b")
