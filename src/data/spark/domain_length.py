# Command to run this script on the CTIT cluster:
# $ spark-submit --master yarn --deploy-mode cluster --packages com.databricks:spark-csv_2.10:1.5.0 src/data/spark/domain_length.py

from pyspark import SparkContext

sc = SparkContext(appName="Alexa Domains")

sc.setLogLevel("ERROR")

rdd = sc.wholeTextFiles('/user/s1962523/agg-alexa/part-*')
rdd = rdd.flatMap(lambda tup: tup[1].split())
rdd = rdd.map(lambda line: (len(line.split(',')[0]), 1))
rdd = rdd.reduceByKey(lambda acc, i: acc + i)
rdd = rdd.map(lambda tup: '%s,%s' % (tup[0], tup[1]))
rdd = rdd.coalesce(1)
rdd.saveAsTextFile('/user/s1962523/agg-alexa-domainlen')