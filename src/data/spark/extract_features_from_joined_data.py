# Command to run this script on the CTIT cluster:
# $ spark-submit --master yarn --deploy-mode cluster --driver-memory 4g --executor-memory 2g src/data/spark/extract_features_from_joined_data.py

from pyspark import SparkContext
import json

sc = SparkContext(appName="Extract Features November 2016")
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
            yield key, alexa_rank


def tokinise_records(line):
    data = json.loads(line)
    domain_name = data['domain']
    date_str = data['date']
    records = data['records']
    # Create a list
    output_list = [len(records)]
    records_str = ';'.join(records)
    for rtype in ['SOA', 'NS', 'A', 'AAAA', 'CNAME', 'MX', 'TXT', 'SPF', 'PTR', 'DNSKEY', 'DS', 'RRSIG', 'CAA', 'NSEC']:
        count = records_str.count(' %s ' % rtype)
        output_list.append(count)
    key = '%s-%s' % (domain_name, date_str)
    return key, output_list


def to_csv_line(input_tup):
    k, v = input_tup
    alexa_rank, counts = v
    dash_idx = k.rfind('-')
    domain_name = k[:dash_idx]
    date_str = k[dash_idx+1:]
    # Generate the output list
    output_list = [domain_name, date_str, str(alexa_rank)] + list(map(str, counts))
    return ','.join(output_list)


in_path = '/user/s1962523/alexa1m-rankings/Alexa_2016-11-*.tar.gz'
rankings_rdd = sc.wholeTextFiles(in_path)
rankings_rdd = rankings_rdd.flatMap(tokenise_rankings)

other_path = '/user/s1962523/openintel-alexa1m-compact/201611*.gz'
records_rdd = sc.textFile(other_path)
records_rdd = records_rdd.map(tokinise_records)

joined_rdd = rankings_rdd.join(records_rdd)
joined_rdd = joined_rdd.map(to_csv_line)

out_path = '/user/s1962523/openintel-alexa1m-ranking-2016-11'
joined_rdd.saveAsTextFile(path=out_path,
                          compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")
