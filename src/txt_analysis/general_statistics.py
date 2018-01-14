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

DIRECTORY_PATH = "/user/s1962523/openintel-alexa1m/openintel-alexa1m-"

parser = argparse.ArgumentParser()
parser.add_argument("--day", help="The day to calculate statistics for. Format: YYYYMMDD", type=str)
arguments = parser.parse_args()
day = arguments.day
results = {"day":day}

def main():
	rdd_pipe = get_necessary_data(day)
	calculator = Calculator(rdd_pipe)
	get_basic_stats_and_filter_data(calculator)
	get_specific_stats(calculator)
	save_results()
	save_leftovers(calculator)

def get_basic_stats_and_filter_data(calculator):
	results["total"] = calculator.size()
	calculator.remove_non_txt_records()
	results["txt"] = calculator.size()
	calculator.remove_empty_text_records()
	results["correct_txt"] = calculator.size()

def get_specific_stats(calculator):
	results["specific"] = calculator.get_specific_stats_for_regex_dictionary(calculator.regexes)

def save_results():
	sc.parallelize([results]).saveAsTextFile("/user/s2012146/" + day + "_results.txt")
	
def save_leftovers(calculator):
	calculator \
		.get_sorted_remaining_records() \
		.map(lambda (text, name): text + "\t" + name) \
		.saveAsTextFile("/user/s2012146/" + day + "_log.txt")

def get_necessary_data(day):
	path_regex = DIRECTORY_PATH + day + "/*json.gz"
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