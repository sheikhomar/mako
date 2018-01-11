

# Command to run this script on the CTIT cluster:
# $ spark-submit --master yarn --deploy-mode cluster --packages com.databricks:spark-csv_2.10:1.5.0 src/data/spark/domain_length_sql.py

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark.sql import functions as func
import time

sc = SparkContext("local", "Alexa Domain Length")
sc.setLogLevel("ERROR")
sqlContext = SQLContext(sc)

schema = StructType([
    StructField("domain", StringType(), False),
    StructField("count", IntegerType(), False),
    StructField("min_rank", IntegerType(), False),
    StructField("max_rank", IntegerType(), False),
    StructField("avg_rank", FloatType(), False),
    StructField("stddev", FloatType(), False),
    StructField("variance", FloatType(), False),
    StructField("skewness", FloatType(), False),
    StructField("kurtosis", FloatType(), False),
])
df = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("inferSchema", "false").schema(schema)
df = df.load('file:///home/s1962523/agg_alexa/part-*')

def str_len(str):
    return len(str)

str_len_udf = udf(str_len, IntegerType())
df = df.withColumn('length', str_len_udf(df['domain']))
df = df.groupby('length').agg(
    func.count('domain').alias('len_count')
)
df.write.format('com.databricks.spark.csv').save('file:///home/s1962523/alexa_domainlen')
