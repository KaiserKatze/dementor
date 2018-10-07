#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os.path
import datetime
import sqlite3
import concurrent.futures
import logging

import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

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
DEFAULT_SEPERATOR = '------------------------------'
############################################################################

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

class FutureHandler:

    def __init__(self, parent):
        self.parent = parent

    def getTable(self):
        return self.parent.table

    def setTable(self, table):
        self.parent.table = table

class SHFE:

    PRODUCTIDS = ( 'CU', 'AL', 'ZN', 'PB', 'NI', 'SN', 'AU', 'AG', 'RB', 'WR', 'HC', 'FU', 'BU', 'RU', )
    HOSTNAME = 'www.shfe.com.cn'
    URL_REFERER = 'http://www.shfe.com.cn/statements/dataview.html?paramid=delaymarket_cu'
    DEFAULT_COLUMNS = ( 'instrumentId', 'refSettlementPrice', 'updown', )
    DEFAULT_SQL_PATH = 'shfe.sqlite3'
    DEFAULT_SQL_TABLE_NAME = 'shfe'
    DEFAULT_INDEX_NAME = 'index'

    def __init__(self):
        self.spider = Spiders.TimePriceSpider(self)
        self.parser = Parsers.TimePriceParser(self)

    def startSpider(self,
                dsrc=datetime.date(2002, 1, 1),
                ddst=datetime.date.today(),
            ):
        sdsrc = dsrc.strftime('%Y-%m-%d')
        sddst = ddst.strftime('%Y-%m-%d')
        print(f'SHFE spider starts from `{sdsrc}` to `{sddst}` ...')

        with Session(host=SHFE.HOSTNAME, referer=SHFE.URL_REFERER) as session,\
                ThreadPoolExecutor() as executor:
            try:
                self.loadTable()
                self.parser.traverseDate(\
                        executor = executor,
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

if __name__ == '__main__':
    shfe = SHFE()

    import sys
    import signal

    def handler(signal, frame):
        shfe.saveTable()
        sys.exit(0)

    signal.signal(signal.SIGINT, handler)

    try:
        shfe.startSpider(dsrc = datetime.date(2018, 1, 1), ddst = datetime.date(2018, 1, 2))
    finally:
        shfe.saveTable()
