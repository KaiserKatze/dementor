#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os.path
import datetime
import dateutil.rrule as rrule
import enum
import json
import sqlite3
import collections
import logging
import typing
import concurrent.futures

import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

import dtutil

LOGGING_FILE = 'futures.log'
logging.basicConfig(\
        filename = LOGGING_FILE,
        filemode = 'w',
        level = logging.INFO,
        format = '[%(asctime)s] %(levelname)s %(message)s',
        datefmt = '%m/%d/%Y %I:%M:%S %p',
    )
logger = logging.getLogger(__name__)
DEFAULT_SEPERATOR = '------------------------------'

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

def ThreadPoolExecutor():
    MAX_WORKERS = 4
    return concurrent.futures.ThreadPoolExecutor(max_workers = MAX_WORKERS)

def plot(dataframe, instrumentId):
    df[df['instrumentId'] == instrumentId].plot(\
            x = 'reportDate',
            y = 'refSettlementPrice',
            kind = 'line',
            title = instrumentId,
        )
    plt.show()

class SpiderException(Exception):
    pass

class SHFE:

    @enum.unique
    class Suffix(enum.Enum):
        default = 'defaultTimePrice.dat'
        main    = 'mainTimePrice.dat'
        daily   = 'dailyTimePrice.dat'

    PRODUCTIDS = ( 'CU', 'AL', 'ZN', 'PB', 'NI', 'SN', 'AU', 'AG', 'RB', 'WR', 'HC', 'FU', 'BU', 'RU', )
    HOSTNAME = 'www.shfe.com.cn'
    URL_REFERER = 'http://www.shfe.com.cn/statements/dataview.html?paramid=delaymarket_cu'
    DEFAULT_COLUMNS = ( 'instrumentId', 'refSettlementPrice', 'updown', )
    DEFAULT_SQL_PATH = 'shfe.sqlite3'
    DEFAULT_SQL_TABLE_NAME = 'shfe'
    DEFAULT_INDEX_NAME = 'index'

    def __init__(self):
        pass

    def startSpider(self,
                dsrc=datetime.date(2002, 1, 1),
                ddst=datetime.date.today(),
            ):
        with Session(host=SHFE.HOSTNAME, referer=SHFE.URL_REFERER) as session,\
                ThreadPoolExecutor() as executor:
            try:
                self.loadTable()
                self.traverseDate(\
                        executor = executor,
                        callback = lambda dt: self.fetchData(session, dt, SHFE.Suffix.daily),
                        dsrc = dsrc,
                        ddst = ddst,
                    )

            finally:
                self.saveTable()

    def loadTable(self,
                force_new=False,
            ):
        self.table = None

        if not force_new and os.path.isfile(SHFE.DEFAULT_SQL_PATH):
            self.table = self.loadTableFromSqlite()

        if self.table is None:
            self.table = self.createNewTable()

        assert self.table is not None, 'Fail to load table!'

        # Post-loading processing
        self.table.index.name = SHFE.DEFAULT_INDEX_NAME

    def createNewTable(self):
        return pd.DataFrame(\
                columns=self.DEFAULT_COLUMNS,
                dtype=np.int64,
            )

    def loadTableFromSqlite(self):
        try:
            with sqlite3.connect(SHFE.DEFAULT_SQL_PATH) as conn:
                columnNameSeperator = ', '
                sIndexColumnName = SHFE.DEFAULT_INDEX_NAME
                columns = [sIndexColumnName, *self.DEFAULT_COLUMNS]
                # wrap each column name with
                columns = map(lambda column: ''.join(column.join(['`',] * 2)), columns)
                columnNames = columnNameSeperator.join(columns)
                tableName = SHFE.DEFAULT_SQL_TABLE_NAME

                df = pd.read_sql_query(\
                        f'''SELECT {columnNames}
                            FROM {tableName}''',
                        conn,
                        index_col = sIndexColumnName,
                        parse_dates = [
                            sIndexColumnName,
                        ],
                    )
                # TODO validate data integrity

                logger.info(f'Load table successfully!\n{DEFAULT_SEPERATOR}\nLoaded table:\n{df}\n{DEFAULT_SEPERATOR}')

                return df

        except pd.io.sql.DatabaseError as e:
            logger.exception(e)
            raise

        except Exception as e:
            logger.exception(e)
            return None

    def saveTable(self):
        if self.table is None:
            raise TypeError('Field `table` is None!')

        with sqlite3.connect(SHFE.DEFAULT_SQL_PATH) as conn:
            self.table.to_sql(\
                    SHFE.DEFAULT_SQL_TABLE_NAME,
                    conn,
                    schema = None,
                    if_exists = 'replace',
                    index = True,
                    index_label = SHFE.DEFAULT_INDEX_NAME,
                    chunksize = None,
                    dtype = {
                        'instrumentId': 'TEXT',
                        'refSettlementPrice': 'INTEGER',
                        'updown': 'INTEGER',
                    },
                )

    def parseChunk(self,
                reportDate: datetime.date,
                chunk,
            ):
        if not reportDate:
            raise TypeError('Argument `reportDate` is not specified!')
        if not isinstance(reportDate, datetime.date):
            raise TypeError(f'Argument `reportDate` has invalid type: `{type(reportDate)}`!')
        if not chunk:
            raise TypeError('Argument `chunk` is not specified!')

        u'''合约名称'''
        instrumentId = chunk['INSTRUMENTID'].strip().upper()
        if not instrumentId[:2] in SHFE.PRODUCTIDS:
            raise SpiderException(f'Invalid `instrumentId`: {instrumentId}!')

        u'''加权平均价'''
        refSettlementPrice = int(chunk['REFSETTLEMENTPRICE'])

        u'''涨跌'''
        updown = int(round(float(chunk['UPDOWN'])))

        u'''创建新记录（行）'''
        row = pd.Series([
                    instrumentId,
                    refSettlementPrice,
                    updown,
                ],
                index = self.DEFAULT_COLUMNS,
                name = pd.to_datetime(reportDate),
            )

        #logger.info(row)

        return row

    def parseText(self,
                reportDate: datetime.date,
                text: str,
            ):
        if not text:
            raise SpiderException(TypeError('Argument `text` is not specified!'))

        result = json.loads(text)
        chunks = result.get('o_currefprice')
        if not isinstance(chunks, collections.Iterable):
            raise SpiderException('Invalid data structure: invalid chunks found!')

        for chunk in chunks:
            if not isinstance(chunk, dict):
                raise SpiderException('Invalid data structure: invalid chunk found!')

            chunkTime = chunk.get('TIME')
            if not isinstance(chunkTime, str):
                raise SpiderException('Invalid data structure: invalid TIME found!')

            if '9:00-15:00' in chunkTime:
                row = self.parseChunk(reportDate, chunk)
                self.table = self.table.append(row)

    def parseData(self, reportDate, response):
        try:
            text = response.text
            self.parseText(reportDate, text)

        except SpiderException as e:
            sReportDate = reportDate.strftime('%Y-%m-%d')
            logger.info(f'\n{DEFAULT_SEPERATOR}\nreportDate = {sReportDate}\nresponse.text =\n{text}\n{DEFAULT_SEPERATOR}')
            logger.exception(e)

    def generateUrl(self, reportDate, suffix):
        if not reportDate:
            raise TypeError('Argument `reportDate` is not specified!')
        if not isinstance(reportDate, datetime.date):
            raise TypeError(f'Argument `reportDate` has invalid type: `{type(reportDate)}`!')
        if not suffix:
            raise TypeError('Argument `suffix` is not specified!')

        '''假期不开展业务'''
        if dtutil.isWeekend(reportDate) or dtutil.isHoliday(reportDate):
            return None

        '''检查数据库中是否已经有本日记录'''
        row = self.table.loc[pd.to_datetime(reportDate)]
        if row:
            return None

        #logger.info(f'Input parameters:\ndate={reportDate}\nsuffix={suffix.value}\n')

        if isinstance(suffix, SHFE.Suffix):
            suffix = suffix.value

        if isinstance(suffix, str):
            # Example:
            # 'http://www.shfe.com.cn/data/dailydata/ck/20180920mainTimePrice.dat'
            sReportDate = reportDate.strftime('%Y%m%d')
            result = f'http://www.shfe.com.cn/data/dailydata/ck/{sReportDate}{suffix}'
            print('url=', result)
            return result

        else:
            raise TypeError(f'Argument `suffix={suffix}` is invalid!')

    def fetchData(self, session, reportDate, suffix):
        url = self.generateUrl(reportDate, suffix)
        if not url:
            return

        response = session.get(url)

        logger.info(f'Fetching data for `url={url}` ...')

        if response.status_code == 200:
            self.parseData(reportDate, response)

        else:
            logger.error(f'Fail to retrieve request(url={url})!')

    def traverseDate(self,
                executor,
                callback,
                dsrc,
                ddst,
            ):
        if not isinstance(dsrc, datetime.date):
            raise TypeError(f'Argument `dsrc` has invalid type: `{type(dsrc)}`!')
        if not isinstance(ddst, datetime.date):
            raise TypeError(f'Argument `ddst` has invalid type: `{type(ddst)}`!')
        if not dsrc < ddst:
            raise ValueError('Argument `dsrc` should be earlier than `ddst`!')
        if not callback:
            raise TypeError('Argument `callback` is not specified!')

        def TraversalTask(cb, ttdsrc, ttddst):
            for reportDate in rrule.rrule(rrule.DAILY, dtstart=ttdsrc, until=ttddst):
                cb(reportDate)

            logger.info(f'Complete traversal successfully!\n{DEFAULT_SEPERATOR}\nTable:\n{self.table}\n{DEFAULT_SEPERATOR}')

        tasks = []
        for year in range(dsrc.year, ddst.year):
            ndsrc = datetime.date(year, 1, 1)
            nddst = datetime.date(year, 12, 31)
            task = TraversalTask(callback, ndsrc, nddst)
            tasks.append(task)
        ndsrc = datetime.date(ddst.year, 1, 1)
        nddst = ddst
        task = TraversalTask(callback, ndsrc, nddst)
        tasks.append(task)

        executor.submit(task)

if __name__ == '__main__':
    shfe = SHFE()

    import sys
    import signal

    def handler(signal, frame):
        shfe.saveTable()
        sys.exit(0)

    signal.signal(signal.SIGINT, handler)

    try:
        shfe.startSpider(dsrc = datetime.date(2018, 1, 1), ddst = datetime.date(2018, 1, 31))
    finally:
        shfe.saveTable()
