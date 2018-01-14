from pyspark.sql.types import *

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