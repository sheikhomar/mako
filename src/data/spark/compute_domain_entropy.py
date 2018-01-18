# Command to run this script on the CTIT cluster:
# $ spark-submit --master yarn --deploy-mode cluster src/data/spark/compute_domain_entropy.py

from pyspark import SparkContext
import math

sc = SparkContext(appName="Compute Domain Name Entropy")

sc.setLogLevel("ERROR")


# Source: https://answers.splunk.com/answers/13636/calculate-entropy-just-entropy-not-change-in-entropy-like-associate.html
def entropy(string):
    """Calculates the Shannon entropy of a string"""
    prob = [float(string.count(c)) / len(string) for c in dict.fromkeys(list(string))]
    entropy = - sum([p * math.log(p) / math.log(2.0) for p in prob])
    return entropy


# Source: https://answers.splunk.com/answers/13636/calculate-entropy-just-entropy-not-change-in-entropy-like-associate.html
def entropy_ideal(length):
    """Calculates the ideal Shannon entropy of a string with given length"""
    prob = 1.0 / length
    ideal = -1.0 * length * prob * math.log(prob) / math.log(2.0)
    return ideal


def to_tuple(line):
    fields = line.split(',')
    avg_ranking = int(float(fields[4]))
    domain_name = fields[0].lower()
    sld = domain_name[:domain_name.rfind('.')]
    etp = entropy(sld)
    etp_ideal = entropy_ideal(len(sld))
    return domain_name, etp, etp_ideal, avg_ranking


rdd = sc.textFile('/user/s1962523/agg-alexa/part-*')
rdd = rdd.map(to_tuple)
rdd = rdd.filter(lambda t: not t[0].startswith('xn--'))
rdd = rdd.map(lambda t: ','.join(map(str, t)))
rdd = rdd.coalesce(1)
rdd.saveAsTextFile(path='/user/s1962523/agg-alexa-entropy',
                   compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")
