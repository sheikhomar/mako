# -*- coding: utf-8 -*-
import concurrent
import os
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import click
import logging
import urllib.request
import sys
import tarfile
import fastavro as avro
import json
import gzip
import re

from dotenv import find_dotenv, load_dotenv

logger = logging.getLogger(__name__)


def get_paths(dir, extension):
    return [os.path.join(dir, f) for f in os.listdir(dir) if os.path.isfile(os.path.join(dir, f)) and f.endswith(extension)]


def store_schema(reader, out_path):
    avro_schema = reader.schema
    with open(out_path, 'w') as out_file:
        json.dump(avro_schema, out_file, sort_keys=True, indent=2)


def clean_record(record):
    new_record = {}
    for key, value in record.items():
        if value:
            new_record[key] = record[key]
    return new_record


def task_download(url):
    logger.info('Downloading %s' % url)

    # Create target file path
    file_name = url[url.rfind('/') + 1:]
    file_path = os.path.join(data_raw_dir, file_name)

    # Actual download file
    urllib.request.urlretrieve(url, file_path)

    logger.info('Downloaded to %s' % file_path)
    return file_path


def task_extract_archive(archive_path):
    logger.info('Extracting %s...' % archive_path)

    # Generate target path
    file_name = archive_path[archive_path.rfind('/') + 1:]
    file_name_no_ext = file_name[0:file_name.rfind('.')]
    target_dir = os.path.join(data_raw_dir, file_name_no_ext)

    # Extract archive
    tar = tarfile.open(archive_path, 'r:')
    tar.extractall(path=target_dir)
    tar.close()

    logger.info('Extracted to %s...' % target_dir)

    # Clean up. We don't need the file anymore.
    os.remove(archive_path)

    return target_dir


def task_download_and_extract(url):
    downloaded_file_path = task_download(url)
    return task_extract_archive(downloaded_file_path)


def task_convert_avro_file_to_json(file_path):
    out_file_path = file_path.replace('.avro', '.json.gz')
    schema_file_path = file_path.replace('.avro', '.avro-schema.json')
    with open(file_path, 'rb') as in_file:
        logger.info('Converting %s' % file_path)
        with gzip.GzipFile(out_file_path, 'w') as out_file:
            logger.info('Writing to %s' % out_file_path)
            reader = avro.reader(in_file)
            store_schema(reader, schema_file_path)

            i = 0
            json_str = ''
            for record in reader:
                cr = clean_record(record)
                json_str += json.dumps(cr, separators=(',', ':')) + '\n'

                # Write in batches
                if i % 200 == 0:
                    json_bytes = json_str.encode('utf-8')
                    out_file.write(json_bytes)
                    json_str = ''
                    i = 0
                else:
                    i += 1

            # Write the last part of the file
            if len(json_str) > 0:
                json_bytes = json_str.encode('utf-8')
                out_file.write(json_bytes)


    # We don't need the AVRO file anymore
    os.remove(file_path)

    return out_file_path


def handle_completed_extract_task(executor, extract_result):
    logger.info('Extract to %s' % extract_result)

    # Find files to convert
    avro_files = get_paths(extract_result, '.avro')

    # Schedule conversion tasks
    futures = []
    for file in avro_files:
        logger.info('Scheduling convert task for %s' % file)
        future = executor.submit(task_convert_avro_file_to_json, file)
        futures.append(future)

    # Handle tasks as they complete
    for future in concurrent.futures.as_completed(futures):
        try:
            file_path = future.result()
        except Exception as exc:
            logger.error('handle_completed_extract_task: Got an exception: %s' % exc)
            logger.error(exc, exc_info=True)
        else:
            logger.info('Converted file: %s' % file_path)


def handle_completed_download_task(executor):
    # Find files to extract
    archives = get_paths(data_raw_dir, '.tar')

    # Schedule extraction tasks
    futures = []
    for archive in archives:
        logger.info('Scheduling extract job task for %s' % archive)
        future = executor.submit(task_extract_archive, archive)
        futures.append(future)

    # Handle tasks as they complete
    for future in concurrent.futures.as_completed(futures):
        try:
            extract_dir = future.result()
        except Exception as exc:
            logger.error('handle_completed_extract_task: Got an exception: %s' % exc)
            logger.error(exc, exc_info=True)
        else:
            logger.info('Extract to %s' % extract_dir)
            handle_completed_extract_task(executor, extract_dir)


def parse_filters(filters):
    year_dict = {}

    for filter_str in filters:
        year = filter_str[0:4]
        month_day = filter_str[4:]
        if year not in year_dict:
            year_dict[year] = []
        if len(month_day) > 0:
            year_dict[year].append(month_day)

    return year_dict


def get_urls(data_set, filters):
    urls = []
    BASE_URL = 'https://data.openintel.nl/data/'
    m = parse_filters(filters)
    logger.info(m)
    for year in m.keys():
        month_days = m[year]
        index_url = BASE_URL + data_set + '/' + year + '/'

        try:
            logger.info('Dowloading %s' % index_url)
            page = urllib.request.urlopen(index_url)
            html = page.read().decode('utf-8')
        except Exception as exc:
            logger.error('Failed to download %s' % index_url)
            logger.error(exc)
            continue

        logger.info('Parsing %s' % index_url)
        prefix = 'openintel-' + data_set + '-' + year
        if len(month_days) > 0:
            prefix += '(' + '|'.join(month_days) + ')'
        else:
            prefix += '(.)'

        relative_urls = re.findall(r'href=[\'"]?(' + prefix + '[^\'" >]+)', html)
        urls.extend(map(lambda url: index_url + url[0], relative_urls))

    return urls


def process(urls, no_threads=1, no_processes=2):
    logger.info("Begin processing...")
    # Note that we use two different executors since threads are good for I/O tasks,
    # while processes are good for CPU-bound tasks.
    with ThreadPoolExecutor(max_workers=no_threads) as threadExecutor:
        futures = []
        for url in urls:
            logger.info('Scheduling download task for %s' % url)
            future = threadExecutor.submit(task_download_and_extract, url)
            futures.append(future)

        with ProcessPoolExecutor(max_workers=no_processes) as processExecutor:
            # Handle tasks as they complete
            for future in concurrent.futures.as_completed(futures):
                try:
                    extract_dir = future.result()
                except Exception as exc:
                    logger.error('main: Got an exception: %s' % exc)
                    logger.error(exc, exc_info=True)
                else:
                    handle_completed_extract_task(processExecutor, extract_dir)

    logger.info('Work done')


@click.command()
@click.option('--data_set', '-d', help='The dataset to prepare. Default: alexa1m', default='alexa1m',  type=click.Choice(['alexa1m', 'open-tld']))
@click.option('--filters', '-f', multiple=True, help='Format is YYYYMMDD where MM and DD are optional. Default: 20170101', default=['20170101'])
@click.option('--threads', '-t', help='Number of simultaneous downloads. Default: 1', default=1)
@click.option('--cores', '-c', help='Number of CPU cores to use. Default: 2', default=2)
def main(data_set, filters, threads, cores):
    logger.info('Processing data set "%s" with filters [%s]' % (data_set, filters))
    urls = get_urls(data_set, filters)
    logger.info('Filter generated %s URL(s)' % len(urls))
    process(urls, threads, cores)


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
    data_raw_dir = os.path.join(project_dir, 'data', 'raw')

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
