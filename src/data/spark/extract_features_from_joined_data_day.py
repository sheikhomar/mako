# Command to run this script on the CTIT cluster:
# $ spark-submit --master yarn --deploy-mode cluster src/data/spark/extract_features_from_joined_data_day.py

from pyspark import SparkContext
import json
from datetime import datetime


def convert_rank(rank_field):
    # For some odd reason, Spark always reads the first line of a .tar.gz
    # file incorrectly and include gibberish. This means that the rank field
    # of the top 1 domain contains weird characters followed by '1'.
    if len(rank_field) > 100:
        return 1
    return int(rank_field)


def tokenise_rankings(input_tup):
    file_name, file_contents = input_tup
    lines = file_contents.split()
    for line in lines:
        fields = line.split(',')
        # The first 2 lines are gibberish and do not have a comma
        # Therefore, we have this check.
        if len(fields) == 2:
            domain_name = fields[1]
            alexa_rank = convert_rank(fields[0])
            yield domain_name, alexa_rank


def tokinise_records(line):
    data = json.loads(line)
    domain_name = data['domain']
    records = data['records']
    # Create a list
    output_list = [len(records)]
    records_str = ';'.join(records)
    for rtype in ['SOA', 'NS', 'A', 'AAAA', 'CNAME', 'MX', 'TXT', 'SPF', 'PTR', 'DNSKEY', 'DS', 'RRSIG', 'CAA', 'NSEC']:
        count = records_str.count(' %s ' % rtype)
        output_list.append(count)
    return domain_name, output_list


def to_csv_line(input_tup):
    domain_name, v = input_tup
    alexa_rank, counts = v
    # Generate the output list
    output_list = [domain_name, str(alexa_rank)] + list(map(str, counts))
    return ','.join(output_list)


DATE = datetime.strptime('2016-11-01', '%Y-%m-%d')

sc = SparkContext(appName='Extract Features %s' % DATE.strftime('%Y-%m-%d'))
sc.setLogLevel("ERROR")

in_path = '/user/s1962523/alexa1m-rankings/Alexa_%s.tar.gz' % DATE.strftime('%Y-%m-%d')
# Note, we use 'sc.wholeTextFiles' instead of 'sc.textFile' to handle
# strange bug in Spark when reading .tar.gz files.
rankings_rdd = sc.wholeTextFiles(in_path)
rankings_rdd = rankings_rdd.flatMap(tokenise_rankings)

other_path = '/user/s1962523/openintel-alexa1m-compact/%s*.gz' % DATE.strftime('%Y%m%d')
records_rdd = sc.textFile(other_path)
records_rdd = records_rdd.map(tokinise_records)

joined_rdd = rankings_rdd.join(records_rdd)
joined_rdd = joined_rdd.map(to_csv_line)

out_path = '/user/s1962523/openintel-alexa1m-ranking-' % DATE.strftime('%Y-%m-%d')
joined_rdd.saveAsTextFile(path=out_path,
                          compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")
