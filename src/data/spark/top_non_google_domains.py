# Command to run this script on the CTIT cluster:
# $ spark-submit --master yarn --deploy-mode cluster src/data/spark/top_non_google_domains.py

from pyspark import SparkContext

sc = SparkContext(appName="Top Non-Google Domains")
sc.setLogLevel("ERROR")
TOP = 100


def convert_rank(str):
    if len(str) > 100:
        return 1
    return int(str)


def filter_top(tup):
    file_name, file_contents = tup
    uidx = file_name.rfind('_')
    date_str = file_name[uidx+1:uidx+11].replace('-', '')
    lines = file_contents.split()
    result = []
    for line in lines:
        fields = line.split(',')
        if len(fields) == 2:
            domain_name = fields[1]
            if 'google' in domain_name:
                continue
            alexa_rank = convert_rank(fields[0])
            # result.append((domain_name, ranking, date_str))
            our_rank = len(result) + 1
            result.append('%s,%s,%s,%s' % (domain_name, alexa_rank, our_rank, date_str))
        if len(result) > TOP:
            break
    return result


in_path = '/user/s1962523/alexa1m-rankings/Alexa_*.tar.gz'
out_path = '/user/s1962523/alexa1m-rankings-top-%s' % TOP
rdd = sc.wholeTextFiles(in_path)
rdd = rdd.flatMap(filter_top)
rdd.saveAsTextFile(path=out_path,
                   compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")
