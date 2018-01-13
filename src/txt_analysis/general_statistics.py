from pyspark import SparkContext
from pyspark.sql import SQLContext
import argparse

sc = SparkContext(appName="TXT analysis")
sc.setLogLevel("ERROR")
sqlContext = SQLContext(sc)

DIRECTORY_PATH = "/user/s1962523/openintel-alexa1m/openintel-alexa1m-"

parser = argparse.ArgumentParser()
parser.add_argument("--day", help="The day to calculate statistics for. Format: YYYYMMDD", type=str)
arguments = parser.parse_args()
day = arguments.day

def main():
	from calculations import Calculator
	rdd_pipe = get_necessary_data(day)
	calculator = Calculator(rdd)
	get_basic_stats_and_filter_data(calculator)

def get_basic_stats_and_filter_data(calculator):
	total = calculator.size()
	calculator.remove_non_txt_records()
	txt = calculator.size()
	calculator.remove_empty_text_records()
	correct_txt = calculator.size()
	print("Total: {}, TXT: {}, correct TXT: {}".format(total, txt, correct_txt))

def get_necessary_data(day):
	path_regex = DIRECTORY_PATH + day + "/*json.gz"
	rdd_pipe = (
		sqlContext
		.read.json(path_regex)
		.select("response_type", "response_name", "txt_text")
		.rdd.map(tuple)
	)
	return rdd_pipe

main()