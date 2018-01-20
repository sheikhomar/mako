from pyspark import SparkContext
from pyspark.sql import SQLContext
import argparse
from schema import *

sc = SparkContext(appName="SOA errors analysis")
sc.setLogLevel("ERROR")
sqlContext = SQLContext(sc)

DIRECTORY_PATH = "/user/s1962523/openintel-alexa1m/openintel-alexa1m-"

parser = argparse.ArgumentParser()
parser.add_argument(
    "--day",
    help="The day to calculate statistics for. Format: YYYYMMDD",
    type=str
)
arguments = parser.parse_args()
day = arguments.day


def main():
    path_regex = DIRECTORY_PATH + day + "/*json.gz"
    rdd = sqlContext.read.option("inferSchema", "false").schema(schema).json(
            path_regex
        ).rdd
    rdd = rdd.map(lambda r: (r, 1)).reduceByKey(lambda a, b: a + b)
    rdd.saveAsTextFile(
        "/user/s1991574/record_types/{day}".format(day=day)
    )


main()