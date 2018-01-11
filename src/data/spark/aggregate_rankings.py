from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark.sql import functions as func

def convert_rank(str):
    if len(str) > 100:
        return 1
    return int(str)

sc = SparkContext(appName="Aggregate Alexa Rankings January 2017")
sc.setLogLevel("ERROR")
sqlContext = SQLContext(sc)

schema = StructType([
    StructField("rank_str", StringType(), False),
    StructField("domain", StringType(), False)
])
df = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("inferSchema", "false").schema(schema)
df = df.load('/user/s1962523/alexa1m-rankings/Alexa_2017-01-*.tar.gz')

convert_rank_udf = udf(convert_rank, IntegerType())
df = df.withColumn('rank', convert_rank_udf(df['rank_str'])).drop("rank_str")
df = df.groupby('domain').agg(
        func.count('rank').alias('count'),
        func.min('rank').alias('min_rank'),
        func.max('rank').alias('max_rank'),
        func.avg('rank').alias('avg_rank'),
        func.stddev('rank').alias('stddev_rank'),
        func.variance('rank').alias('variance_rank'),
        func.skewness('rank').alias('skewness_rank'),
        func.kurtosis('rank').alias('kurtosis_rank')
        )

df.write.format('com.databricks.spark.csv').save('/user/s1962523/agg-alexa-2017-01')