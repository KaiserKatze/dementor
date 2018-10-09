#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import concurrent.futures

import requests

from decorators import package

DEFAULT_HTTP_REQUEST_HEADERS_CONFIG_PATH = 'http-request-headers.config.json'

@package
def getHttpRequestHeadersConfig():
    data = {}
    with open(DEFAULT_HTTP_REQUEST_HEADERS_CONFIG_PATH,
                mode = 'r',
                encoding = 'utf-8',
            ) as fd:
        text = fd.read()
        data = json.loads(text)

    return data

def Session(host, referer):
    data = getHttpRequestHeadersConfig()

    headers = {
        **data,
        'Host': host,
        'Referer': referer,
    }
    session = requests.Session()
    session.headers.update(headers)

    return session

def ThreadPoolExecutor():
    MAX_WORKERS = 4
    return concurrent.futures.ThreadPoolExecutor(max_workers = MAX_WORKERS)

class Target:
    def __init__(self):
        self._session = None
        self._executor = ThreadPoolExecutor()
        self._table = None

    @property
    def session(self):
        return self._session

    @property
    def executor(self):
        return self._executor

    @property
    def table(self):
        return self._table

    @table.setter
    def table(self, value):
        self._table = table

if __name__ == '__main__':
    config = getHttpRequestHeadersConfig()
    print('HTTP request headers config:', config)
