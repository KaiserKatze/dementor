import sqlite3
import datetime
import logging
import json
import re
import concurrent.futures
import os.path

import pandas as pd
import numpy as np

here = os.path.abspath(os.path.dirname(__file__))

try:
    from targets.shfe import SHFE
    from target import Session
except ImportError as e1:
    from futures import SHFE, Session, ThreadPoolExecutor

LOGGING_FILE = 'futures.log'
logging.basicConfig(\
        filename=LOGGING_FILE,
        filemode='w',
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S %p',
    )
logger = logging.getLogger(__name__)

def test_timeprice():
    uri = '20180920dailyTimePrice.dat'
    #uri = './20180920defaultTimePrice.dat'
    uri = os.path.join(here, uri)

    shfe = SHFE()
    shfe.loadTable(force_new=True)

    #############################################
    # Check DataFrame Initialization
    #############################################

    assert shfe.table is not None, 'DataFrame `shfe.table` is not properly initialized!'
    assert shfe.table.size == 0, 'DataFrame `shfe.table` should be EMPTY!'

    with open(uri, mode='r', encoding='utf-8') as file:
        text = file.read()
        shfe.parseText(datetime.date(2018, 9, 20), text)

        #############################################
        # Check Data Interpretation
        #############################################

        assert shfe.table.size > 0, 'DataFrame `shfe.table` should NOT be EMPTY!'

        logger.info(f'Complete parsing!\nTable:\n{shfe.table}')

        table = shfe.table.copy(deep=True)
        shfe.saveTable()

        #############################################
        # Check Load/Save Consistency
        #############################################

        logger.info('Double-checking!')

        shfe.loadTable()

        assert shfe.table is not None, 'Fail to reload dataFrame `shfe.table`!'
        pd.testing.assert_frame_equal(table,
                shfe.table,
                check_dtype=False,
            ) # 'Fail to keep load/save consistency!'

        #############################################
        # Check Spider
        #############################################

        shfe.loadTable(force_new=True)

        print(f'shfe.table={shfe.table}')
        assert isinstance(shfe.table, pd.DataFrame), f'Output DataFrame `boc.table` has invalid type {type(shfe.table)}!'
        assert shfe.table.size == 0, 'New DataFrame `shfe.table` should be EMPTY!'

        dsrc = datetime.date(2018, 1, 1)
        ddst = datetime.date(2018, 1, 2)

        session = None
        executor = None

        try:
            HOSTNAME = 'www.shfe.com.cn'
            URL_REFERER = 'http://www.shfe.com.cn/statements/dataview.html?paramid=delaymarket_cu'
            session = Session(HOSTNAME, URL_REFERER)
        except:
            pass

        try:
            executor = ThreadPoolExecutor()
        except:
            pass

        suffix = 'dailyTimePrice.dat'
        paramsFetchData = {
            'session': session,
        }
        paramsFetchData = { k: v for k, v in paramsFetchData.items() if v is not None }
        callback = lambda dt: shfe.fetchData(reportDate=dt, suffix=suffix, **paramsFetchData)
        paramsTraverseDate = {
            'executor': executor,
            'callback': callback,
        }
        paramsTraverseDate = { k: v for k, v in paramsTraverseDate.items() if v is not None }
        futures = shfe.traverseDate(dsrc=dsrc, ddst=ddst, **paramsTraverseDate)

        concurrent.futures.wait(futures, timeout=None, return_when=concurrent.futures.ALL_COMPLETED)

        try:
            if session:
                session.close()
        except:
            pass

        print(f'shfe.table={shfe.table}')
        assert isinstance(shfe.table, pd.DataFrame), f'Output DataFrame `boc.table` has invalid type {type(shfe.table)}!'
        assert shfe.table.size > 0, 'Output DataFrame `shfe.table` should NOT be EMPTY!'
        date_index = shfe.table.index[0]
        iyear = date_index.year
        imonth = date_index.month
        iday = date_index.day
        assert iyear == 2018 and imonth == 1 and (iday == 1 or iday == 2), 'Date output conflicts with input!'

def test_stock():
    path = '20181009dailystock.dat'
    path = os.path.join(here, path)
    with open(path, mode='r', encoding='utf-8') as file:
        text = file.read()
        data = json.loads(text)

        # 交易日期
        o_tradingday = data['o_tradingday']
        o_tradingday = datetime.datetime.strptime(o_tradingday, '%Y%m%d')
        assert o_tradingday.year == 2018
        assert o_tradingday.month == 10
        assert o_tradingday.day == 9
        # 年度期数 —— 2018年第185期(总第 2436 期)
        # 返回 185
        o_issueno = data['o_issueno']
        o_issueno = int(o_issueno)
        assert o_issueno == 185
        # 年度期数 —— 2018年第185期(总第 2436 期)
        o_totalissueno = data['o_totalissueno']
        o_totalissueno = int(o_totalissueno)
        assert o_totalissueno == 2436
        # 数据
        o_cursor = data['o_cursor']
        assert o_cursor is not None
        assert isinstance(o_cursor, list)
        assert len(o_cursor) > 0

        columns = [
            '品种',
            '地区',
            '仓库',
            '期货',
            '增减',
        ]

        df = pd.DataFrame(\
                columns=columns,
                dtype=np.int64,
            )
        for entry in o_cursor:
            try:
                # 品种
                # 形如('铜$$COPPER')
                e_varname = entry['VARNAME']
                #print(f'e_varname = {e_varname!r}')
                assert re.match(r'[^\x00-\x7F]+\$\$[A-Z]+', e_varname,) is not None
                e_varname = e_varname[:e_varname.index('$$')]

                # 形如(0)
                e_varsort = entry['VARSORT']

                # 地区
                # 形如('上海$$Shanghai')
                e_regname = entry['REGNAME']
                #print(f'e_regname = {e_regname!r}')
                assert not e_regname or re.match(r'[^\x00-\x7F]+\$\$[a-zA-Z]+', e_regname,) is not None
                try:
                    e_regname = e_regname[:e_regname.index('$$')]
                except:
                    continue

                # 形如(0)
                e_regsort = entry['REGSORT']

                # 仓库
                # 形如('期晟公司$$Qisheng')
                e_whabbrname = entry['WHABBRNAME']
                #print(f'e_whabbrname = {e_whabbrname!r}')
                if 'Total' in e_whabbrname or 'Subtotal' in e_whabbrname:
                    logger.info(f'Skip ({e_whabbrname!r}) ...')
                    continue
                try:
                    e_whabbrname = e_whabbrname[:e_whabbrname.index('$$')]
                except:
                    continue

                # 形如(14)
                e_whrows = entry['WHROWS']

                # 形如('2')
                e_wghtunit = entry['WGHTUNIT']

                # 期货
                # 形如(326)
                e_wrtwghts = entry['WRTWGHTS']
                #print(f'e_wrtwghts = {e_wrtwghts!r}')
                assert isinstance(e_wrtwghts, int)

                # 增减
                # 形如(0)
                e_wrtchange = entry['WRTCHANGE']
                #print(f'e_wrtchange = {e_wrtchange!r}')
                assert isinstance(e_wrtchange, int)

                # 形如(15)
                e_roworder = entry['ROWORDER']

                # 形如('0')
                e_rowstatus = entry['ROWSTATUS']

                row = pd.Series([e_varname, e_regname, e_whabbrname, e_wrtwghts, e_wrtchange,], index=columns)
                #print('Row:')
                #print(row)
                df = df.append(row, ignore_index=True)
            except KeyError as e:
                raise

        print('Data:')
        print(df)
