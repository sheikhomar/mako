# -*- coding: utf-8 -*-
import os
import click
import logging
import subprocess
from colorama import Fore, Back, Style

from url_builder import UrlBuilder

logger = logging.getLogger(__name__)


def _color(input, color):
    return color + input + Style.RESET_ALL


@click.command()
@click.option('--data_set', '-d',
              help='The dataset to prepare. Default: alexa1m',
              default='alexa1m',
              type=click.Choice(['alexa1m', 'open-tld']))
@click.option('--filters', '-f',
              multiple=True,
              help='Format is YYYYMMDD where MM and DD are optional. Default: 2016,2017',
              default=['201611'])
@click.argument('path',
                type=click.Path(exists=True))
def main(data_set, filters, path):
    logger.info('Processing data set "%s" with filters [%s]' % (data_set, filters))
    ub = UrlBuilder()
    urls = ub.get_urls(data_set, filters)
    logger.info('Filter generated %s URL(s)' % len(urls))

    local_dir = click.format_filename(path)
    logger.info("File name %s " % local_dir)

    remote_paths = map(lambda url: url[url.rfind('/') + 1:url.rfind('.')], urls)

    # local_paths = os.listdir(local_dir)
    # diff = list(set(remote_paths) - set(local_paths))
    # logger.info('Size: %s' % len(diff))
    # logger.info(diff)

    for name in remote_paths:
        print(name + ':', end='')

        local_path = os.path.join(local_dir, name)

        if os.path.exists(local_path):
            files = [f for f in os.listdir(local_path) if f.endswith('.json.gz')]
            no_files = len(files)
            c = Fore.GREEN if no_files == 11 else Fore.YELLOW
            print(' Local files: ', end='')
            print(_color(str(no_files), c), end='')

            print(' Integrity check: ', end='')
            for file in files:
                full_gz_path = os.path.join(local_path, file)
                return_code = subprocess.call(['gunzip', '-t', full_gz_path])
                good = return_code == 0
                msg = _color('✓', Fore.GREEN) if good else _color('✕', Fore.RED)
                print(msg, end='', flush=True)
        else:
            print(_color(' Not found', Fore.RED), end='')

        print(Style.RESET_ALL)


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)

    main()
