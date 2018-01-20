# Command to run this:
# spark-submit --master yarn --deploy-mode cluster --num-executors 11 --py-files calculations.py,../schema_definition/schema.py general_statistics.py --day YYYYMMDD

from pyspark import SparkContext
from pyspark.sql import SQLContext
import argparse
import json

from calculations import Calculator
from schema import *

sc = SparkContext(appName="TXT analysis")
sc.setLogLevel("ERROR")
sqlContext = SQLContext(sc)

parser = argparse.ArgumentParser()
parser.add_argument("--day", help="The day to calculate statistics for. Format: YYYYMMDD", type=str)
arguments = parser.parse_args()
day = arguments.day
path_regex = "/user/s1962523/openintel-alexa1m/openintel-alexa1m-" + day + "/*json.gz"

def main():
	calculator = Calculator(get_data())
	get_basic_stats_and_filter_data(calculator)
	get_specific_stats(calculator)
	get_sensitive_data(calculator)

def get_basic_stats_and_filter_data(calculator):
	save_rdd(calculator.count_basic_stats(), "basic")
	calculator.filter_data()

def get_specific_stats(calculator):
	save_rdd(calculator.get_specific_stats(), "specific")

def get_sensitive_data(calculator):
	save_rdd(calculator.get_sensitive_data(), "sensitive")

def save_rdd(rdd, filename):
	rdd.saveAsTextFile("/user/s2012146/" + day + "_" + filename)

def get_data():
	rdd_pipe = (
		sqlContext
		.read.option("inferSchema", "false")
		.schema(schema)
		.json(path_regex)
		.select("response_type", "response_name", "txt_text")
		.rdd.map(tuple)
	)
	return rdd_pipe

main()