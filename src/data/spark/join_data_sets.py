# Command to run this script on the CTIT cluster:
# $ spark-submit --master yarn --deploy-mode cluster src/data/spark/join_data_sets.py

from pyspark import SparkContext
import json

sc = SparkContext(appName="Joining two data sets")
sc.setLogLevel("ERROR")


def convert_rank(rank_field):
    if len(rank_field) > 100:
        return 1
    return int(rank_field)


def tokenise_rankings(input_tup):
    file_name, file_contents = input_tup
    uidx = file_name.rfind('_')
    date_str = file_name[uidx+1:uidx+11].replace('-', '')
    lines = file_contents.split()
    for line in lines:
        fields = line.split(',')
        if len(fields) == 2:
            domain_name = fields[1]
            alexa_rank = convert_rank(fields[0])
            key = '%s-%s' % (domain_name, date_str)
            tup = (key, alexa_rank)
            yield tup


def tokinise_records(line):
    data = json.loads(line)
    domain_name = data['domain']
    date_str = data['date']
    records = data['records']
    key = '%s-%s' % (domain_name, date_str)
    return key, records


in_path = '/user/s1962523/alexa1m-rankings/Alexa_2016-11-01*.tar.gz'
rankings_rdd = sc.wholeTextFiles(in_path)
rankings_rdd = rankings_rdd.flatMap(tokenise_rankings)

other_path = '/user/s1962523/openintel-alexa1m-compact/20161101*.gz'
records_rdd = sc.textFile(other_path)
records_rdd = records_rdd.map(tokinise_records)

joined_rdd = rankings_rdd.join(records_rdd)
joined_rdd.take(10)
