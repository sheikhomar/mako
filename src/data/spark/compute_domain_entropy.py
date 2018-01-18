# Command to run this script on the CTIT cluster:
# $ spark-submit --master yarn --deploy-mode cluster src/data/spark/compute_domain_entropy.py

from pyspark import SparkContext
import re
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


def transform(line):
    fields = line.split(',')
    domain_name = fields[0].lower()
    sld = domain_name[:domain_name.rfind('.')]
    etp = entropy(sld)
    etp_ideal = entropy_ideal(len(sld))
    out = [domain_name, str(etp), str(etp_ideal)]
    return ','.join(out)


rdd = sc.textFile('/user/s1962523/agg-alexa/part-*')
rdd = rdd.map(transform)
rdd.saveAsTextFile('/user/s1962523/agg-alexa-entropy')
