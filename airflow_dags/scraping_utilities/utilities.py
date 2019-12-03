import requests
from lxml.html import fromstring
from more_itertools import random_permutation
from itertools import cycle


# Some code lifted from a tutorial on creating rotating IP set for scraping
#   https://www.scrapehero.com/how-to-rotate-proxies-and-ip-addresses-using-python-3/


def get_proxies():
    url = 'https://free-proxy-list.net/'
    response = requests.get(url)
    parser = fromstring(response.text)
    proxies = set()

    for i in parser.xpath('//tbody/tr')[:10]:
        if i.xpath('.//td[7][contains(text(),"yes")]'):
            # Grabbing IP and corresponding PORT
            proxy = ":".join([i.xpath('.//td[1]/text()')[0], i.xpath('.//td[2]/text()')[0]])
            proxies.add(proxy)

    return random_permutation(list(proxies))


def get_proxy_cycle():
    return cycle(get_proxies())
