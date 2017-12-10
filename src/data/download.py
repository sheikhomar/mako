# -*- coding: utf-8 -*-
import os
import click
import logging
import urllib.request
import sys
import tarfile

from dotenv import find_dotenv, load_dotenv

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

@click.command()
def main():
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)

    file_name = 'openintel-alexa1m-20170101.tar'
    url = 'https://data.openintel.nl/data/alexa1m/2017/' + file_name
    data_raw_path = os.path.join(project_dir, 'data', 'raw')
    file_path = os.path.join(data_raw_path, file_name)

    logger.info('Downloading %s to %s' % (url, file_path))
    urllib.request.urlretrieve(url, file_path, reporthook)

    logger.info('Extracting %s...' % file_path)
    tar = tarfile.open(file_path, 'r:')

    tar.extractall(path=data_raw_path)
    tar.close()

    logger.info('Done!')


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
