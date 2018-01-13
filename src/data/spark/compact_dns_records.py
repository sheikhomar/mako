# This script groups, sanitises and compacts DNS records by domain names and date.
#
# Command to run this script on the CTIT cluster:
# $ spark-submit --master yarn --deploy-mode cluster src/data/spark/compact_dns_records.py

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import json
import datetime

schema = StructType([
    StructField("query_type", StringType(), False),
    StructField("query_name", StringType(), False),
    StructField("response_type", StringType(), True),
    StructField("response_name", StringType(), True),
    StructField("timestamp", LongType(), False),
    StructField("worker_id", IntegerType(), False),
    StructField("status_code", IntegerType(), False),
    StructField("ip4_address", StringType(), True),
    StructField("ip6_address", StringType(), True),
    StructField("country", StringType(), True),
    StructField("as", StringType(), True),
    StructField("as_full", StringType(), True),
    StructField("cname_name", StringType(), True),
    StructField("dname_name", StringType(), True),
    StructField("mx_address", StringType(), True),
    StructField("mx_preference", IntegerType(), True),
    StructField("mxset_hash_algorithm", StringType(), True),
    StructField("mxset_hash", StringType(), True),
    StructField("ns_address", StringType(), True),
    StructField("nsset_hash_algorithm", StringType(), True),
    StructField("nsset_hash", StringType(), True),
    StructField("txt_text", StringType(), True),
    StructField("txt_hash_algorithm", StringType(), True),
    StructField("txt_hash", StringType(), True),
    StructField("ds_key_tag", IntegerType(), True),
    StructField("ds_algorithm", IntegerType(), True),
    StructField("ds_digest_type", IntegerType(), True),
    StructField("ds_digest", StringType(), True),
    StructField("dnskey_flags", IntegerType(), True),
    StructField("dnskey_protocol", IntegerType(), True),
    StructField("dnskey_algorithm", IntegerType(), True),
    StructField("dnskey_pk_rsa_n", StringType(), True),
    StructField("dnskey_pk_rsa_e", StringType(), True),
    StructField("dnskey_pk_rsa_bitsize", IntegerType(), True),
    StructField("dnskey_pk_eccgost_x", StringType(), True),
    StructField("dnskey_pk_eccgost_y", StringType(), True),
    StructField("dnskey_pk_dsa_t", StringType(), True),
    StructField("dnskey_pk_dsa_q", StringType(), True),
    StructField("dnskey_pk_dsa_p", StringType(), True),
    StructField("dnskey_pk_dsa_g", StringType(), True),
    StructField("dnskey_pk_dsa_y", StringType(), True),
    StructField("dnskey_pk_eddsa_a", StringType(), True),
    StructField("dnskey_pk_wire", StringType(), True),
    StructField("nsec_next_domain_name", StringType(), True),
    StructField("nsec_owner_rrset_types", StringType(), True),
    StructField("nsec3_hash_algorithm", IntegerType(), True),
    StructField("nsec3_flags", IntegerType(), True),
    StructField("nsec3_iterations", IntegerType(), True),
    StructField("nsec3_salt", StringType(), True),
    StructField("nsec3_next_domain_name_hash", StringType(), True),
    StructField("nsec3_owner_rrset_types", StringType(), True),
    StructField("nsec3param_hash_algorithm", IntegerType(), True),
    StructField("nsec3param_flags", IntegerType(), True),
    StructField("nsec3param_iterations", IntegerType(), True),
    StructField("nsec3param_salt", StringType(), True),
    StructField("spf_text", StringType(), True),
    StructField("spf_hash_algorithm", StringType(), True),
    StructField("spf_hash", StringType(), True),
    StructField("soa_mname", StringType(), True),
    StructField("soa_rname", StringType(), True),
    StructField("soa_serial", LongType(), True),
    StructField("soa_refresh", LongType(), True),
    StructField("soa_retry", LongType(), True),
    StructField("soa_expire", LongType(), True),
    StructField("soa_minimum", LongType(), True),
    StructField("rrsig_type_covered", StringType(), True),
    StructField("rrsig_algorithm", IntegerType(), True),
    StructField("rrsig_labels", IntegerType(), True),
    StructField("rrsig_original_ttl", LongType(), True),
    StructField("rrsig_signature_inception", LongType(), True),
    StructField("rrsig_signature_expiration", LongType(), True),
    StructField("rrsig_key_tag", IntegerType(), True),
    StructField("rrsig_signer_name", StringType(), True),
    StructField("rrsig_signature", StringType(), True),
    StructField("cds_key_tag", IntegerType(), True),
    StructField("cds_algorithm", IntegerType(), True),
    StructField("cds_digest_type", IntegerType(), True),
    StructField("cds_digest", StringType(), True),
    StructField("cdnskey_flags", IntegerType(), True),
    StructField("cdnskey_protocol", IntegerType(), True),
    StructField("cdnskey_algorithm", IntegerType(), True),
    StructField("cdnskey_pk_rsa_n", StringType(), True),
    StructField("cdnskey_pk_rsa_e", StringType(), True),
    StructField("cdnskey_pk_rsa_bitsize", IntegerType(), True),
    StructField("cdnskey_pk_eccgost_x", StringType(), True),
    StructField("cdnskey_pk_eccgost_y", StringType(), True),
    StructField("cdnskey_pk_dsa_t", StringType(), True),
    StructField("cdnskey_pk_dsa_q", StringType(), True),
    StructField("cdnskey_pk_dsa_p", StringType(), True),
    StructField("cdnskey_pk_dsa_g", StringType(), True),
    StructField("cdnskey_pk_dsa_y", StringType(), True),
    StructField("cdnskey_pk_eddsa_a", StringType(), True),
    StructField("cdnskey_pk_wire", StringType(), True),
    StructField("caa_flags", IntegerType(), True),
    StructField("caa_tag", StringType(), True),
    StructField("caa_value", StringType(), True),
    StructField("tlsa_usage", IntegerType(), True),
    StructField("tlsa_selector", IntegerType(), True),
    StructField("tlsa_matchtype", IntegerType(), True),
    StructField("tlsa_certdata", StringType(), True),
    StructField("ptr_name", StringType(), True)
])


def convert_record(obj, domain_name):
    resp_name = obj['response_name']
    resp_type = obj['response_type']
    if resp_name is None:
        # Ignore artificial records such as NSHASH, TXTHASH, etc.
        # These records do not have a response_name
        return []
    name = resp_name
    if domain_name in resp_name:
        name = resp_name.replace(domain_name + '.', '')
        if name.endswith('.'):
            name = name[:-1]
        if name == '':
            name = '@'
    arr = []
    if resp_type == 'SOA':
        mname = obj['soa_mname']
        rname = obj['soa_rname']
        retry = obj['soa_retry']
        minimum = obj['soa_minimum']
        serial = obj['soa_serial']
        expire = obj['soa_expire']
        refresh = obj['soa_refresh']
        arr.append('%s %s %s %s %s %s %s %s %s' % (resp_name, resp_type, mname, rname, serial, refresh, retry, expire, minimum))
    elif resp_type == 'NS':
        arr.append('%s %s %s' % (name, resp_type, obj['ns_address']))
    elif resp_type == 'AAAA':
        arr.append('%s %s %s' % (name, resp_type, obj['ip6_address']))
    elif resp_type == 'A':
        arr.append('%s %s %s' % (name, resp_type, obj['ip4_address']))
    elif resp_type == 'TXT':
        arr.append('%s %s %s' % (name, resp_type, obj['txt_text']))
    elif resp_type == 'SPF':
        arr.append('%s %s %s' % (name, resp_type, obj['spf_text']))
    elif resp_type == 'PTR':
        arr.append('%s %s %s' % (name, resp_type, obj['ptr_text']))
    elif resp_type == 'CNAME':
        arr.append('%s %s %s' % (name, resp_type, obj['cname_name']))
    elif resp_type == 'MX':
        mx_pref = obj['mx_preference']
        mx_addr = obj['mx_address']
        arr.append('%s %s %s %s' % (name, resp_type, mx_pref, mx_addr))
    elif resp_type == 'RRSIG':
        arr.append('%s %s %s %s %s %s (%s %s %s %s %s)' % (
            name,
            resp_type,
            obj['rrsig_type_covered'],
            obj['rrsig_algorithm'],
            obj['rrsig_labels'],
            obj['rrsig_original_ttl'],
            obj['rrsig_signature_expiration'],
            obj['rrsig_signature_inception'],
            obj['rrsig_key_tag'],
            obj['rrsig_signer_name'],
            obj['rrsig_signature'])
        )
    elif resp_type == 'DS':
        arr.append('%s %s %s %s %s %s' % (
            name,
            resp_type,
            obj['ds_key_tag'],
            obj['ds_algorithm'],
            obj['ds_digest_type'],
            obj['ds_digest'])
        )
    elif resp_type == 'DNSKEY':
        arr.append('%s %s %s %s %s' % (
            name,
            resp_type,
            obj['dnskey_flags'],
            obj['dnskey_protocol'],
            obj['dnskey_algorithm'])
        )
    elif resp_type == 'CAA':
        arr.append('%s %s %s %s %s' % (
            name,
            resp_type,
            obj['caa_flags'],
            obj['caa_tag'],
            obj['caa_value'])
        )
    elif resp_type == 'NSEC':
        arr.append('%s %s %s %s' % (
            name,
            resp_type,
            obj['nsec_next_domain_name'],
            obj['nsec_owner_rrset_types'])
        )
    else:
        arr.append('%s %s' % (resp_name, resp_type))
    return arr


def tuplise(row):
    query_name = row['query_name']
    if query_name.startswith('www.'):
        query_name = query_name[4:]  # Remove 'www.' prefix
    if query_name.endswith('.'):
        query_name = query_name[:-1]
    date = datetime.datetime.fromtimestamp(row['timestamp'] / 1000)
    key = '%s-%s' % (query_name, date.strftime('%Y%m%d'))
    record = convert_record(row, query_name)
    return key, record


def sort_by_type(record):
    return record.split(' ')[1]


def to_json(tup):
    key, records = tup
    records = sorted(list(set(records)), key=sort_by_type)
    sep = key.rfind('-')
    domain = key[:sep]
    date = key[sep+1:]
    new_obj = {
        'domain': domain,
        'date': date,
        'records': records
    }
    return json.dumps(new_obj)


sc = SparkContext(appName="Compacts DNS Records November 2017")
sc.setLogLevel("ERROR")
sqlContext = SQLContext(sc)

in_path  = '/user/s1962523/openintel-alexa1m/openintel-alexa1m-201711/*.json.gz'
out_path = '/user/s1962523/alexa1m-compact-201711'
df = sqlContext.read.option("inferSchema", "false").schema(schema).json(in_path)
rdd = df.rdd
rdd = rdd.map(tuplise)
rdd = rdd.reduceByKey(lambda a, n: a + n)
rdd = rdd.map(to_json)
rdd.saveAsTextFile(path=out_path,
                   compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")
