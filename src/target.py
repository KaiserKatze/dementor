#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import json
import concurrent.futures
import os.path
import sqlite3
import logging
import dateutil.rrule as rrule

import requests

from decorators import package

import pandas as pd
import numpy as np

from decorators import package, dbgfunc, malfunc
import dtutil

############################################################################
LOGGING_FILE = 'futures.log'
logging.basicConfig(\
        filename=LOGGING_FILE,
        filemode='w',
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S %p',
    )
logger = logging.getLogger(__name__)
############################################################################

DEFAULT_HTTP_REQUEST_HEADERS_CONFIG_PATH = 'http-request-headers.config.json'

@package
def getHttpRequestHeadersConfig():
    data = {}
    path = os.path.join(GetRootPath(), DEFAULT_HTTP_REQUEST_HEADERS_CONFIG_PATH)
    with open(path,
                mode='r',
                encoding='utf-8',
            ) as fd:
        text = fd.read()
        data = json.loads(text)

    return data

def Session(host: str=None,
            referer: str=None,
        ):
    data = getHttpRequestHeadersConfig()

    headers = {
        **data,
        'Host': host,
        'Referer': referer,
    }
    session = requests.Session()
    session.headers.update(headers)

    return session

@package
def ThreadPoolExecutor():
    MAX_WORKERS = 4
    return concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS)

@package
def GetRootPath():
    return os.path.dirname(os.path.abspath(__file__))

class Target:

    DEFAULT_SQL_PATH = os.path.join(GetRootPath(), 'db.sqlite3')
    DEFAULT_INDEX_NAME = 'index'

    @classmethod
    def GetDefaultTableName(cls):
        return cls.__name__.lower()

    @classmethod
    def createNewTable(cls, columns):
        return pd.DataFrame(\
                columns=columns,
                dtype=np.int64,
            )

    @classmethod
    def loadTableFromSqlite(cls, tableName, columns):
        try:
            with sqlite3.connect(cls.DEFAULT_SQL_PATH) as conn:
                columnNameSeperator = ', '
                backquote = '`'
                sIndexColumnName = cls.DEFAULT_INDEX_NAME
                columns = [sIndexColumnName, *columns]
                # wrap each column name with backquotes
                backquotes = [backquote,] * 2
                columns = map(lambda column: ''.join(column.join(backquotes)), columns)
                sColumnNames = columnNameSeperator.join(columns)

                df = pd.read_sql_query(\
                        f'''SELECT {sColumnNames}
                            FROM {tableName}''',
                        conn,
                        index_col=sIndexColumnName,
                        parse_dates=[
                            sIndexColumnName,
                        ],
                    )
                # TODO validate data integrity

                logger.info(f'''Load table successfully!\n{df}''')

                return df

        except pd.io.sql.DatabaseError as e:
            logger.exception(e)
            raise

        except Exception as e:
            logger.exception(e)
            return None

    def __init__(self,
                session=None,
                tableName=None,
            ):
        self._session = session or Session(\
            host=self.HOSTNAME,
            referer=self.URL_REFERER,
        )
        self._executor = ThreadPoolExecutor()
        self._tableName = tableName or self.GetDefaultTableName()
        self._table = None

    def __del__(self):
        try:
            if self._session:
                self._session.close()
        except:
            pass

    @property
    def session(self) -> requests.Session:
        return self._session

    @property
    def executor(self) -> concurrent.futures.ThreadPoolExecutor:
        return self._executor

    @property
    def table(self) -> pd.DataFrame:
        return self._table

    @table.setter
    def table(self, value: pd.DataFrame):
        self._table = value

    def append_row(self, row: pd.Series):
        if self.table is None:
            raise AttributeError('Field `table` has not been initialized yet!')
        if not isinstance(row, pd.Series):
            raise TypeError(f'Argument `row` has invalid type: `{type(row)}`!')

        rowId = row.name or len(self.table) # assert no error here
        self.table.loc[rowId] = row

    def loadTable(self,
                columns: list,
                force_new: bool=False,
            ) -> bool:
        if not columns:
            raise SpiderException(TypeError('Argument `columns` is not specified!'))

        if force_new:
            table = None

        elif os.path.isfile(self.DEFAULT_SQL_PATH):
            tableName = self._tableName
            table = self.loadTableFromSqlite(tableName, columns)

        if table is None:
            table = self.createNewTable(columns)

        assert table is not None, 'Fail to load table!'

        # Post-loading processing
        table.index.name = self.DEFAULT_INDEX_NAME

        self.table = table

        return True

    def saveTable(self) -> bool:
        table = self.table
        if table is None:
            raise TypeError('Field `table` is None!')

        with sqlite3.connect(self.DEFAULT_SQL_PATH) as conn:
            tableName = self._tableName
            table.to_sql(\
                    tableName,
                    conn,
                    schema=None,
                    if_exists='replace',
                    index=True,
                    index_label=self.DEFAULT_INDEX_NAME,
                    chunksize=None,
                    dtype={
                        'instrumentId': 'TEXT',
                        'refSettlementPrice': 'INTEGER',
                        'updown': 'INTEGER',
                    },
                )

            return True

class BaseParser:

    def parseData(self, reportDate, response):
        try:
            text = response.text
            self.parseText(reportDate, text, response=response)

        except SpiderException as e:
            sReportDate = reportDate.strftime('%Y-%m-%d')
            logger.info(f'''
                    reportDate = {sReportDate}
                    response.text =
                    {text}
                    ''')
            logger.exception(e)

    def parseText(self,
                reportDate: datetime.date,
                text: str,
                **kwargs,
            ):
        if not text:
            raise SpiderException(TypeError('Argument `text` is not specified!'))

        try:
            data = json.loads(text)
            self.parseJson(reportDate, data)
        except:
            logger.info('Invalid data structure: fail to parse response as json!')

            soup = BeautifulSoup(response, 'lxml')
            self.parseHtml(reportDate, soup)

    def parseJson(self, reportDate: datetime.date, data: dict):
        pass

    def parseHtml(self, reportDate: datetime.date, soup):
        pass

class BaseSpider:

    def generateUrl(self, reportDate, **kwargs) -> tuple:
        '''
        @return <tuple>: the first entry 'skip' indicates `fetchData` should skip the given `reportDate`;
                        the second entry 'url' defines the URL that `fetchData` needs
        '''
        if not reportDate:
            raise TypeError('Argument `reportDate` is not specified!')
        if not isinstance(reportDate, datetime.date):
            raise TypeError(f'Argument `reportDate` has invalid type: `{type(reportDate)}`!')

        sReportDate = reportDate.strftime('%Y-%m-%d')

        logger.info(f'Try to generate URL for {sReportDate!r} ...')

        try:
            '''假期不开展业务'''
            if dtutil.isWeekend(reportDate) or dtutil.isHoliday(reportDate):
                logger.info(f'Skip ({sReportDate}) due to weekend/holiday!')
                return True, None

            '''检查数据库中是否已经有本日记录'''
            table = self.table
            pdReportDate = pd.to_datetime(reportDate)
            try:
                row = table.loc[pdReportDate]
            except KeyError:
                pass
            else:
                logger.info(f'Skip ({sReportDate}) due to existing document!\n{row}')
                return True, None
        except:
            raise

        return False, None

    def fetchData(self, reportDate, **kwargs):
        skip, url = self.generateUrl(reportDate, **kwargs)
        logger.info(f'generateUrl() -> (skip={skip}, url={url!r},)')
        if skip:
            return

        session = self.session
        response = session.get(url)

        logger.info(f'Fetching data for `url={url}` ...')

        status_code = response.status_code
        if response.ok:
            self.parseData(reportDate, response)
        else:
            logger.error(f'Fail to retrieve request(url={url})!')

    def traverseDate(self, dsrc, ddst, callback=None) -> [concurrent.futures.Future]:
        if not isinstance(dsrc, datetime.date):
            raise TypeError(f'Argument `dsrc` has invalid type: `{type(dsrc)}`!')
        if not isinstance(ddst, datetime.date):
            raise TypeError(f'Argument `ddst` has invalid type: `{type(ddst)}`!')
        if not dsrc < ddst:
            raise ValueError('Argument `dsrc` should be earlier than `ddst`!')
        if not callable(callback):
            raise TypeError(f'Argument `callback` has invalid type: `{type(callback)}`!')

        def years():
            '''生成器：从起点日期到终点日期，按年份切分；有助于按年份调试解析异常'''
            ndsrc = dsrc
            while ndsrc < ddst:
                year = ndsrc.year
                nddst = datetime.date(year, 12, 31)
                if nddst < ddst:
                    yield ndsrc, nddst
                    ndsrc = datetime.date(year + 1, 1, 1)
                else:
                    yield ndsrc, ddst
                    break

        def TraversalTask(ttdsrc, ttddst):
            '''封装任务函数，开袋即食'''
            print(f'TraversalTask(ttdsrc={ttdsrc}, ttddst={ttddst})')
            def task():
                logger.info(f'Running: TraversalTask(ttdsrc={ttdsrc}, ttddst={ttddst}) ...')
                for reportDate in rrule.rrule(rrule.DAILY, dtstart=ttdsrc, until=ttddst):
                    callback(reportDate)
                logger.info('Complete traversal successfully!')
            return task

        executor = self.executor
        futures = []
        for ttdsrc, ttddst in years():
            task = TraversalTask(ttdsrc, ttddst)
            future = executor.submit(task)
            futures.append(future)

        return futures

class SpiderException(Exception):
    pass

if __name__ == '__main__':
    config = getHttpRequestHeadersConfig()
    print('HTTP request headers config:', config)

    pathRoot = GetRootPath()
    print(f'pathRoot="{pathRoot}"')

    print('All tests passed.')
