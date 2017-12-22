# -*- coding: utf-8 -*-

import logging
import urllib.request
import re

logger = logging.getLogger(__name__)


class UrlBuilder:
    def __init__(self):
        pass

    def _parse_filters(self, filters):
        year_dict = {}

        for filter_str in filters:
            year = filter_str[0:4]
            month_day = filter_str[4:]
            if year not in year_dict:
                year_dict[year] = []
            if len(month_day) > 0:
                year_dict[year].append(month_day)

        return year_dict

    def get_urls(self, data_set, filters):
        urls = []
        BASE_URL = 'https://data.openintel.nl/data/'
        m = self._parse_filters(filters)
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


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    ub = UrlBuilder()
    urls = ub.get_urls('alexa1m', ['2016', '2017'])
    urls.extend(ub.get_urls('open-tld', ['2016', '2017']))

    print(urls)
