# Command to run this script on the CTIT cluster:
# $ spark-submit --master yarn --deploy-mode cluster src/data/spark/domain_features.py

from pyspark import SparkContext
import re
import math

sc = SparkContext(appName="Domain Name Feature Extraction")

sc.setLogLevel("ERROR")

# Generic Top Level Domains
gtld_list = ['com', 'org', 'net', 'edu', 'gov']


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


def tuplise(line):
    fields = line.split(',')
    avg_ranking = int(float(fields[4]))
    domain_name = fields[0].lower()
    tld = domain_name[domain_name.rfind('.')+1:]
    sld = domain_name[:domain_name.rfind('.')]

    domain_len = len(domain_name)
    dot_count = sld.count('.')
    digit_count = len(re.findall(r'\d', sld))
    digit_ratio = 0 if digit_count == 0 else len(sld) / digit_count
    vowel_count = len(re.findall(r'[aeiou]', sld))
    vowel_ratio = 0 if vowel_count == 0 else len(sld) / vowel_count
    etp = entropy(sld)
    etp_ideal = entropy_ideal(len(sld))
    is_gtld = 1 if tld in gtld_list else 0

    return domain_len, dot_count, digit_ratio, vowel_ratio, etp, etp_ideal, is_gtld, avg_ranking


rdd = sc.wholeTextFiles('/user/s1962523/agg-alexa/part-*')
rdd = rdd.flatMap(lambda tup: tup[1].split())
rdd = rdd.map(tuplise)
rdd = rdd.filter(lambda tup: tup[-1] < 100000)
rdd = rdd.map(lambda tup: ','.join(map(str, tup)))
rdd.saveAsTextFile('/user/s1962523/agg-alexa-features-v1')
