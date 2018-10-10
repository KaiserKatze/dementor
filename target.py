#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import concurrent.futures
import os.path
import sqlite3
import logging

import requests

from decorators import package

import pandas as pd
import numpy as np

from decorators import package

############################################################################
LOGGING_FILE = 'futures.log'
logging.basicConfig(\
        filename = LOGGING_FILE,
        filemode = 'w',
        level = logging.INFO,
        format = '[%(asctime)s] %(levelname)s %(message)s',
        datefmt = '%m/%d/%Y %I:%M:%S %p',
    )
logger = logging.getLogger(__name__)
############################################################################

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

def Session(host: str = None,
            referer: str = None,
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
    return concurrent.futures.ThreadPoolExecutor(max_workers = MAX_WORKERS)

class Target:

    DEFAULT_SQL_PATH = 'db.sqlite3'
    DEFAULT_INDEX_NAME = 'index'

    @classmethod
    def GetDefaultTableName(cls):
        return cls.__name__.lower()

    @classmethod
    def createNewTable(cls, columns):
        return pd.DataFrame(\
                columns = columns,
                dtype = np.int64,
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
                        index_col = sIndexColumnName,
                        parse_dates = [
                            sIndexColumnName,
                        ],
                    )
                # TODO validate data integrity

                logger.info(f'''Load table successfully!
                    Loaded table:\n{df}''')

                return df

        except pd.io.sql.DatabaseError as e:
            logger.exception(e)
            raise

        except Exception as e:
            logger.exception(e)
            return None

    def __init__(self,
                session = Session(),
                tableName = None,
            ):
        self._session = session
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
        self._table = table

    def loadTable(self,
                columns: list,
                force_new: bool = False,
            ) -> bool:
        if not columns:
            raise SpiderException(TypeError('Argument `columns` is not specified!'))

        table = self.table

        if not force_new and os.path.isfile(self.DEFAULT_SQL_PATH):
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
                    schema = None,
                    if_exists = 'replace',
                    index = True,
                    index_label = self.DEFAULT_INDEX_NAME,
                    chunksize = None,
                    dtype = {
                        'instrumentId': 'TEXT',
                        'refSettlementPrice': 'INTEGER',
                        'updown': 'INTEGER',
                    },
                )

            return True

class BaseParser:
    pass

class BaseSpider:
    pass

class SpiderException(Exception):
    pass

if __name__ == '__main__':
    config = getHttpRequestHeadersConfig()
    print('HTTP request headers config:', config)

    from targets.shfe.futures import SHFE

    shfe = SHFE()

    import sys
    import signal
    import datetime

    def handler(signal, frame):
        shfe.saveTable()
        sys.exit(0)

    signal.signal(signal.SIGINT, handler)

    try:
        shfe.loadTable()
    except:
        raise
    else:
        try:
            dsrc = datetime.date(2018, 1, 1)
            ddst = datetime.date(2018, 1, 2)
            shfe.startSpider(dsrc = dsrc, ddst = ddst)
        finally:
            shfe.saveTable()

    print('All tests passed.')
