#!/usr/bin/env python
# -*- coding: utf-8 -*-

# `pip install requests`

from datetime import datetime, date
from dateutil.rrule import rrule, DAILY
import enum
import json

import requests

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


def Session(host, referer):
    headers = {
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Host': host,
        'If-Modified-Since': '0',
        'Referer': referer,
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.62 Safari/537.36',
    }
    session = requests.Session()
    session.headers.update(headers)
    return session

def plot(dataframe, instrumentId):
    df[df['instrumentId'] == instrumentId].plot(\
        x='reportDate',
        y='refSettlementPrice',
        kind='line',
        title=instrumentId,)
    plt.show()

class SHFE:
        
    class Suffix(enum.Enum):
        sfe_default = 'defaultTimePrice.dat'
        sfe_main    = 'mainTimePrice.dat'
        sfe_daily   = 'dailyTimePrice.dat'

    LOGGING_FILE = 'futures.log'
    FUTURES_CSV = 'futures.csv'
    INSTRUMENTIDS = ( 'CU', 'AL', 'ZN', 'PB', 'NI', 'SN', 'AU', 'AG', 'RB', 'WR', 'HC', 'FU', 'BU', 'RU', )
    HOSTNAME = 'www.shfe.com.cn'
    URL_REFERER = 'http://www.shfe.com.cn/statements/dataview.html?paramid=delaymarket_cu'

    def __init__(self):
        with Session(host=SHFE.HOSTNAME, referer=SHFE.URL_REFERER) as session:
            df = self.traverseDate(lambda dt: self.fetchData(session, dt, Suffix.sfe_main))

            date = datetime.strptime('2018-7-2', '%Y-%m-%d')
            print('Date:', date.strftime('%Y-%m-%d'))
            #self.fetchData(session, date, Suffix.sfe_default)
            #self.fetchData(session, date, Suffix.sfe_daily)
            self.fetchData(session, date, Suffix.sfe_main)

    def parseData(self, date, response):
        result = json.loads(response.text)
        indexes = ('reportDate', 'instrumentId', 'refSettlementPrice', 'updown')

        with open(LOGGING_FILE, 'a') as log:
            # 日期
            reportDate = date or pd.to_datetime(result['report_date'], format='%Y%m%d', errors='ignore')

            table = pd.DataFrame(index=indexes)
            for chunk in result['o_currefprice']:

                if chunk.get('TIME') == '9:00-15:00':

                    # 品种
                    instrumentId = chunk['INSTRUMENTID'][:2]
                    assert instrumentId in INSTRUMENTIDS, 'Invalid `instrumentId`!'

                    # 加权平均价
                    refSettlementPrice = int(chunk['REFSETTLEMENTPRICE'])

                    # 涨跌
                    updown = float(chunk['UPDOWN'])

                    #log.write('{}\t{}\t{}\t{}\n'.format(reportDate, instrumentId, refSettlementPrice, updown))
                    row = pd.Series([reportDate, instrumentId, refSettlementPrice, updown,], index=indexes)

                    table.append(row)

            #print(table)
            #df = pd.DataFrame(table,
            #    columns=['reportDate', 'instrumentId', 'refSettlementPrice', 'updown',],)
            #print(df)

            #log.write('\n')
            #log.write(json.dumps(result, sort_keys=False, indent=2, ensure_ascii=False))

            return table

    def fetchData(self, session, date, suffix):
        url = self.generateUrl(date, suffix)
        response = session.get(url)
        if response.status_code == 200:
            return self.parseData(date, response)
        else:
            return []

    def traverseDate(callback):
        # 最早的记录，从2002年1月1日开始
        dsrc = date(2002, 1, 1)
        ddst = date.today()
        table = []
        for dt in rrule(DAILY, dtstart=dsrc, until=ddst):
            #print('Fetching data for `{}` ...'.format(dt.strftime('%Y-%m-%d')))
            table.extend(callback(dt))
        df = pd.DataFrame(table,
            columns=['reportDate', 'instrumentId', 'refSettlementPrice', 'updown',],)
        df.to_csv(FUTURES_CSV)
        return df

    def generateUrl(date, suffix):
        if date is None:
            date = datetime.today()
        if suffix is None:
            raise AssertionError('Parameter `suffix` is None!')
        print('Input parameters:')
        print('date=', date)
        print('suffix=', suffix.value)

        if isinstance(suffix, Suffix):
            suffix = suffix.value
        if isinstance(suffix, str):
            result = 'http://www.shfe.com.cn/data/dailydata/ck/{}{}'.format(date.strftime('%Y%m%d'), suffix)
            print('url=', result)
            return result
        else:
            raise AssertionError('Parameter `suffix` is invalid!')