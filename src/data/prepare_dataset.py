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

from dotenv import find_dotenv, load_dotenv

logger = logging.getLogger(__name__)


def get_paths(dir, extension):
    return [os.path.join(dir, f) for f in os.listdir(dir) if os.path.isfile(os.path.join(dir, f)) and f.endswith(extension)]


def store_schema(reader, out_path):
    avro_schema = reader.schema
    with open(out_path, 'w') as out_file:
        json.dump(avro_schema, out_file, sort_keys=True, indent=2)


def clean_record(record):
    for key, value in list(record.items()):
        if value is None:
            del record[key]
        elif isinstance(value, dict):
            clean_record(value)
    return record


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
            for record in reader:
                cr = clean_record(record.copy())
                json_str = json.dumps(cr, separators=(',', ':')) + '\n'
                json_bytes = json_str.encode('utf-8')
                out_file.write(json_bytes)
                i += 1

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


MAX_WORKERS = 4


@click.command()
def main():
    # extract_dir = os.path.join(project_dir, 'data', 'raw', 'openintel-alexa1m-20170107')
    # convert_avro_files_to_json(extract_dir)
    urls = [
        'https://data.openintel.nl/data/alexa1m/2017/openintel-alexa1m-20170115.tar',
        'https://data.openintel.nl/data/alexa1m/2017/openintel-alexa1m-20170116.tar',
        'https://data.openintel.nl/data/alexa1m/2017/openintel-alexa1m-20170117.tar',
        'https://data.openintel.nl/data/alexa1m/2017/openintel-alexa1m-20170118.tar'
    ]

    # Note that we use two different executors since threads are good for I/O tasks,
    # while processes are good for CPU-bound tasks.
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as threadExecutor:
        futures = []
        for url in urls:
            logger.info('Scheduling download task for %s' % url)
            future = threadExecutor.submit(task_download_and_extract, url)
            futures.append(future)

        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as processExecutor:
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