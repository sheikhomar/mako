# -*- coding: utf-8 -*-
import os
import click
import logging
import urllib.request
import sys
import tarfile
import fastavro as avro
import json
import gzip
from multiprocessing import Pool

from dotenv import find_dotenv, load_dotenv

logger = logging.getLogger(__name__)

@click.command()
def main():
    url = 'https://data.openintel.nl/data/alexa1m/2017/openintel-alexa1m-20170104.tar'
    extract_dir = download_and_extract(url)
    logger.info('Done! Extracted to %s...' % extract_dir)

    #extract_dir = os.path.join(project_dir, 'data', 'raw', 'openintel-alexa1m-20170103')
    convert_avro_files_to_json(extract_dir)


# https://stackoverflow.com/questions/13881092/download-progressbar-for-python-3
def reporthook(blocknum, blocksize, totalsize):
    readsofar = blocknum * blocksize
    if totalsize > 0:
        percent = readsofar * 1e2 / totalsize
        s = "\r%5.1f%% %*d / %d" % (
            percent, len(str(totalsize)), readsofar, totalsize)
        sys.stderr.write(s)
        if readsofar >= totalsize: # near the end
            sys.stderr.write("\n")
    else: # total size is unknown
        sys.stderr.write("read %d\n" % (readsofar,))


def download_and_extract(url):
    file_name = url[url.rfind('/') + 1:]
    data_raw_path = os.path.join(project_dir, 'data', 'raw')
    file_path = os.path.join(data_raw_path, file_name)

    logger.info('Downloading %s' % url)
    urllib.request.urlretrieve(url, file_path, reporthook)

    logger.info('Extracting %s...' % file_path)
    tar = tarfile.open(file_path, 'r:')

    file_name_no_ext = file_name[0:file_name.rfind('.')]
    target_dir = os.path.join(data_raw_path, file_name_no_ext)
    tar.extractall(path=target_dir)
    tar.close()

    # We don't need the downloaded file
    os.remove(file_path)

    return target_dir


def convert_avro_files_to_json(path):
    files = [os.path.join(path, f) for f in os.listdir(path) if os.path.isfile(os.path.join(path, f)) and f.endswith('avro')]
    pool = Pool()
    pool.map(convert_avro_file_to_json, files)
    pool.close()
    pool.join()


def convert_avro_file_to_json(file_path):
    out_file_path = file_path.replace('.avro', '.json.gz')
    schema_file_path = file_path.replace('.avro', '.avro-schema.json')
    with open(file_path, 'rb') as in_file:
        logger.info('Converting %s' % file_path)
        with gzip.GzipFile(out_file_path, 'w') as out_file:
            logger.info('Writing to %s' % out_file_path)
            reader = avro.reader(in_file)
            store_schema(reader, schema_file_path)
            i = 0
            for record in reader:
                cr = clean_record(record.copy())
                json_str = json.dumps(cr, separators=(',', ':')) + '\n'
                json_bytes = json_str.encode('utf-8')
                out_file.write(json_bytes)
                i += 1

    # We don't need the AVRO file anymore
    os.remove(file_path)


def store_schema(reader, out_path):
    with open(out_path, 'w') as out_file:
        json.dump(reader.schema, out_file, sort_keys=True, indent=2)


def clean_record(record):
    for key, value in list(record.items()):
        if value is None:
            del record[key]
        elif isinstance(value, dict):
            clean_record(value)
    return record


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()