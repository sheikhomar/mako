# Command to run this script on the CTIT cluster:
# $ spark-submit --master yarn --deploy-mode cluster src/data/spark/length_vs_ranking.py

from pyspark import SparkContext

sc = SparkContext(appName="Len vs Rank")

sc.setLogLevel("ERROR")


def tuplise(line):
    fields = line.split(',')
    domain_len = len(fields[0])
    avg_ranking = int(float(fields[4]))

    return domain_len, avg_ranking


rdd = sc.wholeTextFiles('/user/s1962523/agg-alexa/part-*')
rdd = rdd.flatMap(lambda tup: tup[1].split())
rdd = rdd.map(tuplise)
rdd = rdd.filter(lambda tup: tup[1] < 1000)
rdd = rdd.map(lambda tup: '%s,%s' % (tup[0], tup[1]))
rdd.saveAsTextFile('/user/s1962523/agg-alexa-len-vs-rank')