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

soa_recommended_values = {
    'max': {
        'refresh': 100800,
        'retry': 10800,
        'expire': 3600000,
        'minimum': 216000
    },
    'min': {
        'refresh': 72000,
        'retry': 3600,
        'expire': 2700000,
        'minimum': 129600
    }
}


def calculate_soa_and_assign_results(rdd):
    results = {"day": day}
    mins = soa_recommended_values['min']
    maxs = soa_recommended_values['max']
    for i, elem in enumerate(['refresh', 'retry', 'expire', 'minimum']):
        temprdd = (
            rdd.filter(lambda tupl: tupl[i] is not None).filter(
                lambda tupl: mins[elem] < tupl[i] < maxs[elem]
            )
        )
        results["errored_{}".format(elem)] = temprdd.count()
    return results


def main():
    rdd = get_necessary_data(day)
    results = calculate_soa_and_assign_results(rdd)
    sc.parallelize([results]).saveAsTextFile(
        "/user/s1991574/{day}_results.txt".format(day=day)
    )


def get_necessary_data(day):
    path_regex = DIRECTORY_PATH + day + "/*json.gz"
    rdd_pipe = (
        sqlContext.read.option("inferSchema", "false").schema(schema).json(
            path_regex
        ).select(
            "soa_expire",
            "soa_minimum",
            "soa_retry",
            "soa_refresh"
        ).rdd.map(tuple)
    )
    return rdd_pipe

main()